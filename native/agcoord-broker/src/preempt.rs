//! Bounded preemption of running local checks.
//!
//! Admission is greedy: a queued row starts whenever its complete claim fits this machine.
//! The oldest queued row that lacks only local capacity can therefore keep waiting while
//! later rows keep fitting.  Once that starving row has waited long enough, the broker stops
//! the youngest later checks that together free what it lacks and returns each of them to the
//! queue at its original sequence, so a waiting caller observes only the final attempt.
//!
//! This module owns the durable record of those attempts.  A row has a record only while it
//! is a running check that may still be preempted, or after it was preempted at least once.

use crate::error::Result;
use crate::resources::initial_receipt;
use crate::store::{RunRecord, load_run, map_database_error, now};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// The reserved submission entry with which a check opts out of preemption.
pub const NO_PREEMPT_ENV: &str = "_AGCOORD_NO_PREEMPT";
/// The reason a running check carries while a preemption is stopping it.
pub const PREEMPTED: &str = "preempted";
/// A check that was preempted this many times is never preempted again.
pub const MAX_PREEMPTIONS: u64 = 2;
/// How long a preempted check may take to disappear before its preemption is abandoned.
pub const STOP_BOUND: Duration = Duration::from_secs(5);

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS run_preemptions (
    run_id TEXT PRIMARY KEY,
    attempts INTEGER NOT NULL DEFAULT 1 CHECK (attempts >= 1),
    preempted_by TEXT,
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
    /// Seconds since the stop was requested.
    pub waited: f64,
}

/// The preemption state of the running rows, read with the live scheduler snapshot.
#[derive(Clone, Debug, Default)]
pub struct Live {
    /// Running checks whose submitted environment is retained, so they can be requeued.
    pub preemptible: BTreeSet<String>,
    pub stops: Vec<Stop>,
}

/// The queued row whose preemption is in flight.  Rows submitted after it wait until its
/// victims are gone, so the capacity they release cannot be taken before it starts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hold {
    pub run_id: String,
    pub sequence: i64,
}

/// What the victim's log records about one completed preemption.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Requeued {
    pub preemptor: String,
    pub requested_at: String,
    /// The attempt that was stopped; the requeued row is on the next one.
    pub attempt: u64,
}

/// One row's stored record as its terminal transaction reads it.
struct PendingStop {
    attempts: i64,
    preemptor: Option<String>,
    requested_at: Option<String>,
    environment: Option<String>,
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

/// Whether the submitter opted this check out of preemption.
pub fn opted_out(environment: &BTreeMap<String, String>) -> bool {
    environment
        .get(NO_PREEMPT_ENV)
        .is_some_and(|value| value == "1")
}

/// The attempt record of every row that has one, by run ID.
pub fn attempts(connection: &Connection) -> Result<BTreeMap<String, Attempts>> {
    let mut statement = connection
        .prepare(
            "SELECT run_id, attempts, COALESCE(stopping_for, preempted_by) FROM run_preemptions",
        )
        .map_err(map_database_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                Attempts {
                    attempts: row.get::<_, i64>(1)?.max(1) as u64,
                    preempted_by: row.get(2)?,
                },
            ))
        })
        .map_err(map_database_error)?;
    rows.collect::<std::result::Result<BTreeMap<_, _>, _>>()
        .map_err(map_database_error)
}

/// One row's attempt record.  A row without one is on its first attempt.
pub fn attempts_of(connection: &Connection, run_id: &str) -> Result<Attempts> {
    connection
        .query_row(
            "SELECT attempts, COALESCE(stopping_for, preempted_by)
             FROM run_preemptions WHERE run_id = ?1",
            params![run_id],
            |row| {
                Ok(Attempts {
                    attempts: row.get::<_, i64>(0)?.max(1) as u64,
                    preempted_by: row.get(1)?,
                })
            },
        )
        .optional()
        .map(|found| found.unwrap_or_else(Attempts::first))
        .map_err(map_database_error)
}

/// Read the preemption state of the running rows.  The join starts from the running rows,
/// so a scheduling tick does not pay for the records terminal history retains.
pub fn live(connection: &Connection) -> Result<Live> {
    let mut statement = connection
        .prepare(
            "SELECT p.run_id, p.environment_json IS NOT NULL, p.stopping_for, p.abandoned,
                    (julianday('now') - julianday(p.stop_requested_at)) * 86400.0
             FROM runs r JOIN run_preemptions p ON p.run_id = r.run_id
             WHERE r.status = 'running'",
        )
        .map_err(map_database_error)?;
    let mut rows = statement.query([]).map_err(map_database_error)?;
    let mut selected = Live::default();
    while let Some(row) = rows.next().map_err(map_database_error)? {
        let run_id: String = row.get(0).map_err(map_database_error)?;
        let retained: bool = row.get(1).map_err(map_database_error)?;
        let preemptor: Option<String> = row.get(2).map_err(map_database_error)?;
        match preemptor {
            Some(preemptor) => selected.stops.push(Stop {
                run_id,
                preemptor,
                abandoned: row.get::<_, i64>(3).map_err(map_database_error)? != 0,
                waited: row
                    .get::<_, Option<f64>>(4)
                    .map_err(map_database_error)?
                    .unwrap_or(0.0),
            }),
            None if retained => {
                selected.preemptible.insert(run_id);
            }
            None => {}
        }
    }
    Ok(selected)
}

/// The queued row whose requested stops are still awaited, if any.
pub fn hold(live: &Live, queued: &[RunRecord]) -> Option<Hold> {
    queued
        .iter()
        .filter(|run| {
            live.stops
                .iter()
                .any(|stop| !stop.abandoned && stop.preemptor == run.run_id)
        })
        .min_by_key(|run| run.sequence)
        .map(|run| Hold {
            run_id: run.run_id.clone(),
            sequence: run.sequence,
        })
}

/// Seconds a row has waited since it was submitted.
pub fn waited(connection: &Connection, created_at: &str) -> Result<f64> {
    let waited: Option<f64> = connection
        .query_row(
            "SELECT (julianday('now') - julianday(?1)) * 86400.0",
            params![created_at],
            |row| row.get(0),
        )
        .map_err(map_database_error)?;
    Ok(waited.unwrap_or(0.0).max(0.0))
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

/// Choose the running checks whose claims, once released, let `starving` fit.
///
/// Candidates are taken youngest start first, so the least work is lost, until the freed
/// units cover every shortage.  The selection is then reduced to a minimal covering set by
/// sparing, oldest start first, each victim whose claim is not needed after all.  `None`
/// means the candidates cannot cover the shortage together, so nothing is preempted.
pub fn plan<'a>(
    starving: &RunRecord,
    used: &BTreeMap<String, u64>,
    capacities: &BTreeMap<String, u64>,
    candidates: &[&'a RunRecord],
) -> Option<Vec<&'a RunRecord>> {
    let shortage: BTreeMap<&str, u64> = starving
        .resources
        .iter()
        .filter_map(|(name, units)| {
            let needed = used.get(name).copied().unwrap_or(0).saturating_add(*units);
            let capacity = capacities.get(name).copied().unwrap_or(0);
            needed
                .checked_sub(capacity)
                .filter(|short| *short > 0)
                .map(|short| (name.as_str(), short))
        })
        .collect();
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
            .started_at
            .cmp(&first.started_at)
            .then_with(|| second.sequence.cmp(&first.sequence))
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

/// Return a stopped victim to the queue at its original sequence, on its next attempt.
///
/// The caller holds the transaction that would otherwise make the row terminal, and has
/// already seen its complete process group disappear.  A row whose stop no preemption
/// requested, or whose caller cancelled it meanwhile, is left for that terminal transition.
pub fn requeue(connection: &Connection, run: &RunRecord) -> Result<Option<Requeued>> {
    if !run.cancel_requested {
        return Ok(None);
    }
    let pending = connection
        .query_row(
            "SELECT attempts, stopping_for, stop_requested_at, environment_json
             FROM run_preemptions WHERE run_id = ?1",
            params![run.run_id],
            |row| {
                Ok(PendingStop {
                    attempts: row.get(0)?,
                    preemptor: row.get(1)?,
                    requested_at: row.get(2)?,
                    environment: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(map_database_error)?;
    let Some(PendingStop {
        attempts,
        preemptor: Some(preemptor),
        requested_at,
        environment: Some(environment),
    }) = pending
    else {
        return Ok(None);
    };
    let requeued_at = now(connection)?;
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
                environment,
                run.run_id
            ],
        )
        .map_err(map_database_error)?;
    connection
        .execute(
            "UPDATE run_preemptions
             SET attempts = attempts + 1, preempted_by = stopping_for, stopping_for = NULL,
                 stop_requested_at = NULL, abandoned = 0, environment_json = NULL
             WHERE run_id = ?1",
            params![run.run_id],
        )
        .map_err(map_database_error)?;
    Ok(Some(Requeued {
        preemptor,
        requested_at: requested_at.unwrap_or(requeued_at),
        attempt: attempts.max(1) as u64,
    }))
}

/// Forget a finished row's retained environment and any stop that never completed.  The
/// caller holds the terminal transaction.  A row that was never preempted leaves no record.
pub fn release(connection: &Connection, run_id: &str) -> Result<()> {
    connection
        .execute(
            "DELETE FROM run_preemptions
             WHERE run_id = ?1 AND attempts = 1 AND preempted_by IS NULL",
            params![run_id],
        )
        .map_err(map_database_error)?;
    connection
        .execute(
            "UPDATE run_preemptions
             SET environment_json = NULL, stopping_for = NULL, stop_requested_at = NULL,
                 abandoned = 0
             WHERE run_id = ?1",
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
    use super::plan;
    use crate::resources::initial_receipt;
    use crate::store::RunRecord;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

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

    fn victims(selected: Option<Vec<&RunRecord>>) -> Option<Vec<String>> {
        selected.map(|runs| runs.iter().map(|run| run.run_id.clone()).collect())
    }

    #[test]
    fn the_youngest_starts_are_stopped_until_the_shortage_is_covered() {
        let starving = run("starving", 1, 0, &[("cpu", 6)]);
        let old = run("old", 2, 10, &[("cpu", 2)]);
        let middle = run("middle", 3, 20, &[("cpu", 2)]);
        let young = run("young", 4, 30, &[("cpu", 2)]);
        let capacities = claim(&[("cpu", 8)]);
        // Six of eight CPUs are held, so the starving row lacks four.
        let used = claim(&[("cpu", 6)]);
        assert_eq!(
            victims(plan(
                &starving,
                &used,
                &capacities,
                &[&old, &middle, &young]
            )),
            Some(vec!["young".to_owned(), "middle".to_owned()])
        );
    }

    #[test]
    fn a_victim_whose_claim_is_not_needed_after_all_is_spared() {
        let starving = run("starving", 1, 0, &[("cpu", 4)]);
        let large = run("large", 2, 10, &[("cpu", 4)]);
        let small = run("small", 3, 20, &[("cpu", 1)]);
        let capacities = claim(&[("cpu", 5)]);
        let used = claim(&[("cpu", 5)]);
        // The youngest row frees one CPU; the older one alone frees all four.
        assert_eq!(
            victims(plan(&starving, &used, &capacities, &[&large, &small])),
            Some(vec!["large".to_owned()])
        );
    }

    #[test]
    fn the_younger_of_two_interchangeable_victims_is_the_one_stopped() {
        let starving = run("starving", 1, 0, &[("cpu", 4)]);
        let large = run("large", 2, 10, &[("cpu", 3)]);
        let older = run("older", 3, 20, &[("cpu", 1)]);
        let younger = run("younger", 4, 30, &[("cpu", 1)]);
        let capacities = claim(&[("cpu", 5)]);
        let used = claim(&[("cpu", 5)]);
        assert_eq!(
            victims(plan(
                &starving,
                &used,
                &capacities,
                &[&large, &older, &younger]
            )),
            Some(vec!["younger".to_owned(), "large".to_owned()])
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
}
