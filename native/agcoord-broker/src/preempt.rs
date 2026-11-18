//! Preemption of running local checks.
//!
//! Admission is first fit: a queued row starts whenever its complete claim fits this machine,
//! and nothing is reserved for a row that does not fit yet.  The oldest queued row that lacks
//! only local capacity may therefore keep waiting while later rows keep fitting.  Once it has
//! waited long enough, the broker stops the latest-submitted checks that together free what
//! it lacks and returns each of them to the queue at its original sequence, so a waiting
//! caller observes only the final attempt.
//!
//! Only checks are ever stopped, so preemption bounds no wait: a row whose shortage is held
//! by work that is never stopped keeps waiting for as long as such work keeps starting.
//!
//! This module owns the durable record of those attempts.  A row has a record only while it
//! is a running check that may still be preempted, or after it was preempted at least once.

use crate::error::Result;
use crate::overflow::Pool;
use crate::resources::initial_receipt;
use crate::store::{RunRecord, load_run, map_database_error, now};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// The reserved submission entry with which a check opts out of preemption.
pub const NO_PREEMPT_ENV: &str = "_AGCOORD_NO_PREEMPT";
/// The reason a running check carries while a preemption is stopping it.
pub const PREEMPTED: &str = "preempted";
/// The reason of a check that was stopped for a preemption and could not be requeued,
/// because the resources of the stopped attempt were not reclaimed cleanly.
pub const RECLAIM_FAILED: &str = "preemption-reclaim-failed";
/// A check that was preempted this many times is never preempted again.
pub const MAX_PREEMPTIONS: u64 = 2;
/// How long a victim may outlive the first stop its owner delivered before the preemption
/// is abandoned.
pub const STOP_BOUND: Duration = Duration::from_secs(5);
/// The shortest default wait before a row other than a land may preempt.
const DEFAULT_AFTER_FLOOR: u64 = 300;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS run_preemptions (
    run_id TEXT PRIMARY KEY,
    attempts INTEGER NOT NULL DEFAULT 1 CHECK (attempts >= 1),
    preempted_by TEXT,
    queued_at TEXT,
    stopping_for TEXT,
    stop_requested_at TEXT,
    abandoned INTEGER NOT NULL DEFAULT 0 CHECK (abandoned IN (0, 1)),
    environment_json TEXT
);
";

/// What a public row reports about its attempts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attempts {
    /// The number of the row's current or next attempt, starting at one.
    pub attempts: u64,
    /// The row that most recently preempted this one, including a stop still in progress.
    pub preempted_by: Option<String>,
}

impl Attempts {
    /// A row that was never preempted.
    pub fn first() -> Self {
        Self {
            attempts: 1,
            preempted_by: None,
        }
    }
}

/// One requested stop whose victim has not disappeared yet.
#[derive(Clone, Debug)]
pub struct Stop {
    pub run_id: String,
    pub preemptor: String,
    /// The broker stopped waiting for this victim; it keeps its claim until it is gone.
    pub abandoned: bool,
}

/// The preemption state of the live rows, read with the live scheduler snapshot.
#[derive(Clone, Debug, Default)]
pub struct Live {
    /// Running checks whose submitted environment is retained, so they can be requeued.
    pub preemptible: BTreeSet<String>,
    pub stops: Vec<Stop>,
    /// When each queued row that was preempted returned to the queue.
    requeued_at: BTreeMap<String, String>,
}

impl Live {
    /// When `run` last entered the queue: at its submission, or at its latest requeue.
    pub fn queued_since<'a>(&'a self, run: &'a RunRecord) -> &'a str {
        self.requeued_at
            .get(&run.run_id)
            .map_or(run.created_at.as_str(), String::as_str)
    }

    /// What the preemption that is still stopping its victims holds back, if one is.
    pub fn hold(&self, queued: &[RunRecord]) -> Option<Hold> {
        queued
            .iter()
            .filter(|run| {
                self.stops
                    .iter()
                    .any(|stop| !stop.abandoned && stop.preemptor == run.run_id)
            })
            .min_by_key(|run| run.sequence)
            .map(Hold::of)
    }
}

/// What a preemption keeps later rows from taking while its victims stop.
///
/// The capacity the victims release is meant for the starving row.  A row submitted after it
/// that claims a resource the starving row still lacks would take that capacity first, so
/// such a row waits; every other row is admitted as usual.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hold {
    pub run_id: String,
    pub sequence: i64,
    /// The starving row's complete claim.
    resources: BTreeMap<String, u64>,
}

impl Hold {
    pub fn of(starving: &RunRecord) -> Self {
        Self {
            run_id: starving.run_id.clone(),
            sequence: starving.sequence,
            resources: starving.resources.clone(),
        }
    }

    /// Whether `run` waits behind the starving row, given what this machine holds now.
    pub fn holds(
        &self,
        run: &RunRecord,
        used: &BTreeMap<String, u64>,
        capacities: &BTreeMap<String, u64>,
    ) -> bool {
        run.sequence > self.sequence
            && shortage(&self.resources, used, capacities)
                .keys()
                .any(|name| run.resources.get(*name).is_some_and(|units| *units > 0))
    }
}

/// A requested stop as its victim's terminal transaction finds it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingStop {
    pub preemptor: String,
    pub requested_at: String,
    /// The attempt that is being stopped; a requeued row is on the next one.
    pub attempt: u64,
    environment: String,
}

fn schema_present(connection: &Connection) -> Result<bool> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'run_preemptions'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map(|found| found.is_some())
        .map_err(map_database_error)
}

pub fn ensure_schema(connection: &Connection) -> Result<()> {
    if schema_present(connection)? {
        return Ok(());
    }
    connection.execute_batch(SCHEMA).map_err(map_database_error)
}

/// Bring the records an earlier owner left behind in line with the rows.
///
/// A row that is not running retains no submitted environment and has no stop in flight,
/// and a row that no longer exists has no record.  An owner that ends a row clears both in
/// the same transaction; one that predates this table ends rows without touching it.
pub fn recover(connection: &Connection) -> Result<()> {
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let recovered = connection
        .execute_batch(
            "DELETE FROM run_preemptions WHERE run_id NOT IN (SELECT run_id FROM runs);
             UPDATE run_preemptions
                SET environment_json = NULL, stopping_for = NULL, stop_requested_at = NULL,
                    abandoned = 0
              WHERE run_id NOT IN (SELECT run_id FROM runs WHERE status = 'running');
             DELETE FROM run_preemptions
              WHERE attempts = 1 AND preempted_by IS NULL AND environment_json IS NULL;",
        )
        .map_err(map_database_error);
    match recovered {
        Ok(()) => connection
            .execute_batch("COMMIT")
            .map_err(map_database_error),
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

/// Whether the submitter opted this check out of preemption.
pub fn opted_out(environment: &BTreeMap<String, String>) -> bool {
    environment
        .get(NO_PREEMPT_ENV)
        .is_some_and(|value| value == "1")
}

/// How long a row other than a land waits by default before it may preempt: as long as the
/// slowest overflow pool takes to accept it, and never less than five minutes.
pub fn default_after(pools: &[Pool]) -> Duration {
    Duration::from_secs(
        pools
            .iter()
            .map(|pool| pool.wait_threshold)
            .max()
            .unwrap_or(0)
            .max(DEFAULT_AFTER_FLOOR),
    )
}

fn attempts_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Attempts> {
    Ok(Attempts {
        attempts: row.get::<_, i64>(0)?.max(1) as u64,
        preempted_by: row.get(1)?,
    })
}

const ATTEMPTS_OF: &str = "SELECT attempts, COALESCE(stopping_for, preempted_by)
     FROM run_preemptions WHERE run_id = ?1";

/// The attempt records of `runs`, by run ID.  The read is one indexed lookup per row asked
/// about, however many records terminal history retains.
pub fn attempts<'a>(
    connection: &Connection,
    runs: impl IntoIterator<Item = &'a RunRecord>,
) -> Result<BTreeMap<String, Attempts>> {
    let mut statement = connection
        .prepare(ATTEMPTS_OF)
        .map_err(map_database_error)?;
    let mut selected = BTreeMap::new();
    for run in runs {
        let record = statement
            .query_row(params![run.run_id], attempts_from_row)
            .optional()
            .map_err(map_database_error)?;
        if let Some(record) = record {
            selected.insert(run.run_id.clone(), record);
        }
    }
    Ok(selected)
}

/// One row's attempt record.  A row without one is on its first attempt.
pub fn attempts_of(connection: &Connection, run_id: &str) -> Result<Attempts> {
    connection
        .query_row(ATTEMPTS_OF, params![run_id], attempts_from_row)
        .optional()
        .map(|found| found.unwrap_or_else(Attempts::first))
        .map_err(map_database_error)
}

/// Read the preemption state of the live rows.  The join is driven from the live rows, so
/// a scheduling tick does not pay for the records terminal history retains.
pub fn live(connection: &Connection) -> Result<Live> {
    let mut statement = connection
        .prepare(
            "SELECT p.run_id, r.status, p.environment_json IS NOT NULL, p.stopping_for,
                    p.abandoned, p.queued_at
             FROM runs r CROSS JOIN run_preemptions p ON p.run_id = r.run_id
             WHERE r.status IN ('queued', 'running')",
        )
        .map_err(map_database_error)?;
    let mut rows = statement.query([]).map_err(map_database_error)?;
    let mut selected = Live::default();
    while let Some(row) = rows.next().map_err(map_database_error)? {
        let run_id: String = row.get(0).map_err(map_database_error)?;
        let status: String = row.get(1).map_err(map_database_error)?;
        if status == "queued" {
            let requeued_at: Option<String> = row.get(5).map_err(map_database_error)?;
            if let Some(requeued_at) = requeued_at {
                selected.requeued_at.insert(run_id, requeued_at);
            }
            continue;
        }
        let retained: bool = row.get(2).map_err(map_database_error)?;
        let preemptor: Option<String> = row.get(3).map_err(map_database_error)?;
        match preemptor {
            Some(preemptor) => selected.stops.push(Stop {
                run_id,
                preemptor,
                abandoned: row.get::<_, i64>(4).map_err(map_database_error)? != 0,
            }),
            None if retained => {
                selected.preemptible.insert(run_id);
            }
            None => {}
        }
    }
    Ok(selected)
}

/// Keep a starting local check's submitted environment so that it can be requeued.  The
/// caller holds the admission transaction.  A check that opted out, or that was already
/// preempted `MAX_PREEMPTIONS` times, is not retained and is therefore never a victim.
pub fn retain(connection: &Connection, run: &RunRecord) -> Result<()> {
    if run.kind != "check" || opted_out(&run.environment) {
        return Ok(());
    }
    if attempts_of(connection, &run.run_id)?.attempts > MAX_PREEMPTIONS {
        return Ok(());
    }
    connection
        .execute(
            "INSERT INTO run_preemptions (run_id, environment_json) VALUES (?1, ?2)
             ON CONFLICT(run_id) DO UPDATE SET environment_json = excluded.environment_json",
            params![run.run_id, serde_json::to_string(&run.environment).unwrap()],
        )
        .map_err(map_database_error)?;
    Ok(())
}

/// The units of each claimed resource that are missing for the claim to fit beside what
/// this machine already holds.
fn shortage<'a>(
    resources: &'a BTreeMap<String, u64>,
    used: &BTreeMap<String, u64>,
    capacities: &BTreeMap<String, u64>,
) -> BTreeMap<&'a str, u64> {
    resources
        .iter()
        .filter_map(|(name, units)| {
            let needed = used.get(name).copied().unwrap_or(0).saturating_add(*units);
            let capacity = capacities.get(name).copied().unwrap_or(0);
            needed
                .checked_sub(capacity)
                .filter(|short| *short > 0)
                .map(|short| (name.as_str(), short))
        })
        .collect()
}

/// Choose the running checks whose claims, once released, let `starving` fit.
///
/// Candidates are taken latest submission first, and among equals youngest start first,
/// until the freed units cover every shortage, so the work that is restarted belongs to the
/// jobs that arrived last.  The selection is then reduced to a minimal covering set by
/// sparing, earliest submission first, each victim whose claim is not needed after all.
/// `None` means the candidates cannot cover the shortage together, so nothing is preempted.
pub fn plan<'a>(
    starving: &RunRecord,
    used: &BTreeMap<String, u64>,
    capacities: &BTreeMap<String, u64>,
    candidates: &[&'a RunRecord],
) -> Option<Vec<&'a RunRecord>> {
    let shortage = shortage(&starving.resources, used, capacities);
    if shortage.is_empty() {
        return None;
    }
    let covers = |selected: &[&RunRecord]| {
        shortage.iter().all(|(name, short)| {
            selected
                .iter()
                .map(|run| run.resources.get(*name).copied().unwrap_or(0))
                .fold(0_u64, u64::saturating_add)
                >= *short
        })
    };
    let mut ordered: Vec<&'a RunRecord> = candidates.to_vec();
    ordered.sort_by(|first, second| {
        second
            .sequence
            .cmp(&first.sequence)
            .then_with(|| second.started_at.cmp(&first.started_at))
    });
    let mut selected: Vec<&'a RunRecord> = Vec::new();
    for candidate in ordered {
        selected.push(candidate);
        if covers(&selected) {
            break;
        }
    }
    if !covers(&selected) {
        return None;
    }
    let mut index = selected.len();
    while index > 0 {
        index -= 1;
        let spared = selected.remove(index);
        if !covers(&selected) {
            selected.insert(index, spared);
        }
    }
    Some(selected)
}

/// Durably request that every victim stop for `starving`.
///
/// Nothing changes unless the starving row is still queued and every victim is still the
/// running, preemptible worker the plan was made for; the caller then plans again on a
/// later pass.  The ordinary cancellation path stops a row whose stop was requested here.
pub fn request(
    connection: &Connection,
    starving: &RunRecord,
    victims: &[RunRecord],
) -> Result<bool> {
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let requested = (|| -> Result<bool> {
        if load_run(connection, &starving.run_id)?.status != "queued" {
            return Ok(false);
        }
        let timestamp = now(connection)?;
        for victim in victims {
            let current = load_run(connection, &victim.run_id)?;
            if current.status != "running"
                || current.cancel_requested
                || current.worker_pid.is_none()
                || current.worker_pid != victim.worker_pid
                || current.worker_start_token != victim.worker_start_token
            {
                return Ok(false);
            }
            let marked = connection
                .execute(
                    "UPDATE run_preemptions
                     SET stopping_for = ?1, stop_requested_at = ?2, abandoned = 0
                     WHERE run_id = ?3 AND environment_json IS NOT NULL
                       AND stopping_for IS NULL",
                    params![starving.run_id, timestamp, victim.run_id],
                )
                .map_err(map_database_error)?;
            if marked != 1 {
                return Ok(false);
            }
            connection
                .execute(
                    "UPDATE runs
                     SET cancel_requested = 1, cancel_requested_at = ?1, failure_reason = ?2
                     WHERE run_id = ?3",
                    params![timestamp, PREEMPTED, victim.run_id],
                )
                .map_err(map_database_error)?;
        }
        Ok(true)
    })();
    match requested {
        Ok(true) => {
            connection
                .execute_batch("COMMIT")
                .map_err(map_database_error)?;
            Ok(true)
        }
        Ok(false) => {
            connection
                .execute_batch("ROLLBACK")
                .map_err(map_database_error)?;
            Ok(false)
        }
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

/// Withdraw a requested stop that was never delivered, because the check had already ended
/// on its own.  Its verdict then stands like that of any other finished check, and it is
/// not chosen again while its process group drains.  A caller's cancellation that arrived
/// meanwhile stays in place.
pub fn withdraw(connection: &Connection, run_id: &str) -> Result<()> {
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .map_err(map_database_error)?;
    let withdrawn = (|| -> Result<()> {
        connection
            .execute(
                "UPDATE runs
                 SET cancel_requested = 0, cancel_requested_at = NULL, failure_reason = NULL
                 WHERE run_id = ?1 AND status = 'running' AND failure_reason = ?2",
                params![run_id, PREEMPTED],
            )
            .map_err(map_database_error)?;
        connection
            .execute(
                "UPDATE run_preemptions
                 SET stopping_for = NULL, stop_requested_at = NULL, abandoned = 0,
                     environment_json = NULL
                 WHERE run_id = ?1",
                params![run_id],
            )
            .map_err(map_database_error)?;
        Ok(())
    })();
    match withdrawn {
        Ok(()) => connection
            .execute_batch("COMMIT")
            .map_err(map_database_error),
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

/// The stop a preemption requested for this row, while it is still pending.  A row whose
/// stop no preemption requested, or whose caller cancelled it meanwhile, has none.
pub fn pending_stop(connection: &Connection, run: &RunRecord) -> Result<Option<PendingStop>> {
    if !run.cancel_requested {
        return Ok(None);
    }
    let record = connection
        .query_row(
            "SELECT attempts, stopping_for, stop_requested_at, environment_json
             FROM run_preemptions WHERE run_id = ?1",
            params![run.run_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(map_database_error)?;
    Ok(match record {
        Some((attempts, Some(preemptor), Some(requested_at), Some(environment))) => {
            Some(PendingStop {
                preemptor,
                requested_at,
                attempt: attempts.max(1) as u64,
                environment,
            })
        }
        _ => None,
    })
}

/// Whether stopping an attempt left enforcement state behind: a backend failed to cancel,
/// finish, or clean up what the attempt held.
pub fn reclaim_failed(receipt: &Value) -> bool {
    receipt
        .get("events")
        .and_then(Value::as_array)
        .is_some_and(|events| {
            events.iter().any(|event| {
                event.get("status").and_then(Value::as_str) == Some("failed")
                    && matches!(
                        event.get("stage").and_then(Value::as_str),
                        Some("cancel" | "finish" | "cleanup")
                    )
            })
        })
}

/// Return a stopped victim to the queue at its original sequence, on its next attempt.
///
/// The caller holds the transaction that would otherwise make the row terminal, has seen
/// its complete process group disappear, and has reclaimed its resources cleanly.  The wait
/// before the row may preempt in its turn starts again at this moment.
pub fn requeue(connection: &Connection, run: &RunRecord, stop: &PendingStop) -> Result<()> {
    connection
        .execute(
            "UPDATE runs
             SET status = 'queued', phase = 'queued', started_at = NULL, finished_at = NULL,
                 exit_status = NULL, reported_exit_status = NULL, failure_reason = NULL,
                 gate_exit_status = NULL, worker_pid = NULL, worker_start_token = NULL,
                 cancel_requested = 0, cancel_requested_at = NULL,
                 resource_receipt_json = ?1, resource_state_json = '{}',
                 environment_json = ?2
             WHERE run_id = ?3 AND status = 'running'",
            params![
                serde_json::to_string(&initial_receipt(&run.resources)).unwrap(),
                stop.environment,
                run.run_id
            ],
        )
        .map_err(map_database_error)?;
    connection
        .execute(
            "UPDATE run_preemptions
             SET attempts = attempts + 1, preempted_by = stopping_for, queued_at = ?1,
                 stopping_for = NULL, stop_requested_at = NULL, abandoned = 0,
                 environment_json = NULL
             WHERE run_id = ?2",
            params![now(connection)?, run.run_id],
        )
        .map_err(map_database_error)?;
    Ok(())
}

/// Forget a finished row's retained environment and requeue time.  The caller holds the
/// terminal transaction.  A row that ends while a preemption was still stopping it keeps
/// the name of the row it was stopped for; one that was never preempted leaves no record.
pub fn release(connection: &Connection, run_id: &str) -> Result<()> {
    connection
        .execute(
            "UPDATE run_preemptions
             SET preempted_by = COALESCE(stopping_for, preempted_by), queued_at = NULL,
                 stopping_for = NULL, stop_requested_at = NULL, abandoned = 0,
                 environment_json = NULL
             WHERE run_id = ?1",
            params![run_id],
        )
        .map_err(map_database_error)?;
    connection
        .execute(
            "DELETE FROM run_preemptions
             WHERE run_id = ?1 AND attempts = 1 AND preempted_by IS NULL",
            params![run_id],
        )
        .map_err(map_database_error)?;
    Ok(())
}

/// A caller's own cancellation wins over a preemption that is still stopping the row: the
/// row then ends cancelled instead of returning to the queue.
pub fn caller_cancelled(connection: &Connection, run_id: &str) -> Result<()> {
    connection
        .execute(
            "UPDATE run_preemptions
             SET stopping_for = NULL, stop_requested_at = NULL, abandoned = 0
             WHERE run_id = ?1",
            params![run_id],
        )
        .map_err(map_database_error)?;
    Ok(())
}

/// Stop waiting for one victim.  Its stop request stays, so it is still requeued once it is
/// finally gone; until then it keeps its claim and nothing is admitted in its place.
pub fn abandon(connection: &Connection, run_id: &str) -> Result<bool> {
    connection
        .execute(
            "UPDATE run_preemptions SET abandoned = 1
             WHERE run_id = ?1 AND stopping_for IS NOT NULL AND abandoned = 0",
            params![run_id],
        )
        .map(|changed| changed == 1)
        .map_err(map_database_error)
}

/// Drop the records of rows that `clear` removed.
pub fn forget_cleared(connection: &Connection) -> Result<()> {
    connection
        .execute(
            "DELETE FROM run_preemptions WHERE run_id NOT IN (SELECT run_id FROM runs)",
            [],
        )
        .map_err(map_database_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Hold, default_after, plan, reclaim_failed};
    use crate::overflow::Pool;
    use crate::resources::initial_receipt;
    use crate::store::RunRecord;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;
    use std::time::Duration;

    fn claim(entries: &[(&str, u64)]) -> BTreeMap<String, u64> {
        entries
            .iter()
            .map(|(name, units)| ((*name).to_owned(), *units))
            .collect()
    }

    fn run(run_id: &str, sequence: i64, started_second: u32, claims: &[(&str, u64)]) -> RunRecord {
        let resources = claim(claims);
        RunRecord {
            sequence,
            run_id: run_id.to_owned(),
            status: "running".to_owned(),
            kind: "check".to_owned(),
            phase: "running".to_owned(),
            label: run_id.to_owned(),
            agent: "unnamed".to_owned(),
            repository_id: "repository".to_owned(),
            repository: "repository".to_owned(),
            worktree_id: "worktree".to_owned(),
            checkout: PathBuf::from("/"),
            branch: "branch".to_owned(),
            head_sha: None,
            barrier: false,
            resource_contract: json!({}),
            resource_receipt: initial_receipt(&resources),
            resource_state: BTreeMap::new(),
            resources,
            gate_run_id: None,
            publication_adapter: None,
            publication_request: None,
            failure_reason: None,
            gate_exit_status: None,
            reported_exit_status: None,
            caller_pid: 1,
            command: vec!["true".to_owned()],
            environment: BTreeMap::new(),
            created_at: "2026-01-01T00:00:00.000Z".to_owned(),
            started_at: Some(format!("2026-01-01T00:00:{started_second:02}.000Z")),
            finished_at: None,
            exit_status: None,
            worker_pid: Some(1),
            worker_start_token: Some("token".to_owned()),
            cancel_requested: false,
        }
    }

    fn pool(name: &str, wait_threshold: u64) -> Pool {
        Pool {
            name: name.to_owned(),
            capacities: claim(&[("jobs", 1)]),
            max_claim: BTreeMap::new(),
            executor: vec!["executor".to_owned()],
            wait_threshold,
            max_runtime: 60,
            budget: None,
            cpu_hour_price: None,
            privileged: false,
            capabilities: BTreeSet::new(),
        }
    }

    fn victims(selected: Option<Vec<&RunRecord>>) -> Option<Vec<String>> {
        selected.map(|runs| runs.iter().map(|run| run.run_id.clone()).collect())
    }

    #[test]
    fn the_latest_submissions_are_stopped_until_the_shortage_is_covered() {
        let starving = run("starving", 1, 0, &[("cpu", 6)]);
        let early = run("early", 2, 10, &[("cpu", 2)]);
        let middle = run("middle", 3, 20, &[("cpu", 2)]);
        let late = run("late", 4, 30, &[("cpu", 2)]);
        let capacities = claim(&[("cpu", 8)]);
        // Six of eight CPUs are held, so the starving row lacks four.
        let used = claim(&[("cpu", 6)]);
        assert_eq!(
            victims(plan(
                &starving,
                &used,
                &capacities,
                &[&early, &middle, &late]
            )),
            Some(vec!["late".to_owned(), "middle".to_owned()])
        );
    }

    #[test]
    fn submission_order_decides_before_start_order() {
        let starving = run("starving", 1, 0, &[("cpu", 4)]);
        // The earlier submission waited for something else and so started last.
        let earlier_submitted = run("earlier-submitted", 2, 30, &[("cpu", 1)]);
        let later_submitted = run("later-submitted", 3, 10, &[("cpu", 1)]);
        let capacities = claim(&[("cpu", 5)]);
        let used = claim(&[("cpu", 2)]);
        assert_eq!(
            victims(plan(
                &starving,
                &used,
                &capacities,
                &[&earlier_submitted, &later_submitted]
            )),
            Some(vec!["later-submitted".to_owned()])
        );
    }

    #[test]
    fn a_victim_whose_claim_is_not_needed_after_all_is_spared() {
        let starving = run("starving", 1, 0, &[("cpu", 4)]);
        let large = run("large", 2, 10, &[("cpu", 4)]);
        let small = run("small", 3, 20, &[("cpu", 1)]);
        let capacities = claim(&[("cpu", 5)]);
        let used = claim(&[("cpu", 5)]);
        // The latest row frees one CPU; the earlier one alone frees all four.
        assert_eq!(
            victims(plan(&starving, &used, &capacities, &[&large, &small])),
            Some(vec!["large".to_owned()])
        );
    }

    #[test]
    fn the_later_of_two_interchangeable_victims_is_the_one_stopped() {
        let starving = run("starving", 1, 0, &[("cpu", 4)]);
        let large = run("large", 2, 10, &[("cpu", 3)]);
        let earlier = run("earlier", 3, 20, &[("cpu", 1)]);
        let later = run("later", 4, 30, &[("cpu", 1)]);
        let capacities = claim(&[("cpu", 5)]);
        let used = claim(&[("cpu", 5)]);
        assert_eq!(
            victims(plan(
                &starving,
                &used,
                &capacities,
                &[&large, &earlier, &later]
            )),
            Some(vec!["later".to_owned(), "large".to_owned()])
        );
    }

    #[test]
    fn every_short_resource_must_be_covered() {
        let starving = run("starving", 1, 0, &[("cpu", 4), ("memory", 8)]);
        let compute = run("compute", 2, 10, &[("cpu", 4)]);
        let memory = run("memory", 3, 20, &[("memory", 8)]);
        let capacities = claim(&[("cpu", 4), ("memory", 8)]);
        let used = claim(&[("cpu", 4), ("memory", 8)]);
        assert_eq!(
            victims(plan(&starving, &used, &capacities, &[&compute, &memory])),
            Some(vec!["memory".to_owned(), "compute".to_owned()])
        );
        // Without the memory holder no selection lets the starving row fit.
        assert_eq!(
            victims(plan(&starving, &used, &capacities, &[&compute])),
            None
        );
    }

    #[test]
    fn a_row_that_already_fits_preempts_nothing() {
        let starving = run("starving", 1, 0, &[("cpu", 2)]);
        let other = run("other", 2, 10, &[("cpu", 2)]);
        let capacities = claim(&[("cpu", 4)]);
        let used = claim(&[("cpu", 2)]);
        assert_eq!(
            victims(plan(&starving, &used, &capacities, &[&other])),
            None
        );
    }

    #[test]
    fn a_preemption_holds_only_later_rows_that_claim_what_the_starving_row_lacks() {
        let starving = run("starving", 2, 0, &[("cpu", 4), ("memory", 8)]);
        let hold = Hold::of(&starving);
        let capacities = claim(&[("cpu", 4), ("memory", 16), ("jobs", 4)]);
        // The victims still hold every CPU; memory has room for the starving row.
        let used = claim(&[("cpu", 4), ("memory", 8), ("jobs", 2)]);
        let earlier = run("earlier", 1, 0, &[("cpu", 1)]);
        let compute = run("compute", 3, 0, &[("cpu", 1), ("jobs", 1)]);
        let memory_only = run("memory-only", 4, 0, &[("memory", 4), ("jobs", 1)]);
        assert!(!hold.holds(&earlier, &used, &capacities));
        assert!(hold.holds(&compute, &used, &capacities));
        assert!(!hold.holds(&memory_only, &used, &capacities));
        // Once the victims are gone the starving row lacks nothing, and nothing is held.
        let released = claim(&[("cpu", 0), ("memory", 8), ("jobs", 2)]);
        assert!(!hold.holds(&compute, &released, &capacities));
    }

    #[test]
    fn the_default_wait_is_the_slowest_pool_threshold_and_never_under_five_minutes() {
        assert_eq!(default_after(&[]), Duration::from_secs(300));
        assert_eq!(
            default_after(&[pool("near", 0), pool("far", 120)]),
            Duration::from_secs(300)
        );
        assert_eq!(
            default_after(&[pool("near", 60), pool("far", 900)]),
            Duration::from_secs(900)
        );
    }

    #[test]
    fn only_a_failed_cancel_finish_or_cleanup_is_an_unclean_reclaim() {
        let event = |stage: &str, status: &str| {
            json!({
                "at": "2026-01-01T00:00:00.000Z",
                "backend": "cgroup-v2",
                "resource": "cpu",
                "stage": stage,
                "status": status,
                "code": "leaf-populated",
            })
        };
        let receipt = |events: Vec<serde_json::Value>| json!({"requested": {"cpu": 1}, "applied": {}, "peak": {}, "events": events});
        assert!(!reclaim_failed(&receipt(vec![])));
        assert!(!reclaim_failed(&receipt(vec![
            event("prepare", "applied"),
            event("usage", "failed"),
            event("cancel", "recorded"),
            event("finish", "recorded"),
            event("cleanup", "recorded"),
        ])));
        for stage in ["cancel", "finish", "cleanup"] {
            assert!(reclaim_failed(&receipt(vec![
                event("prepare", "applied"),
                event(stage, "failed"),
            ])));
        }
    }
}
