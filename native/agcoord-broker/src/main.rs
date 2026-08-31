mod broker;
mod error;
mod platform;
mod store;

use broker::{Broker, ServeOptions};
use error::{AppError, Result};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
use store::{PROTOCOL, Paths, PhaseRequest, SubmitRequest};

const NAME: &str = "agcoord-broker";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const IMPLEMENTATION: &str = "rust-native";
const BUILD: &str = env!("AGCOORD_BUILD_ID");
const TARGET: &str = env!("AGCOORD_TARGET");

fn print_help() {
    println!(
        "{NAME} — native AGCoord scheduler and enforcement owner\n\n\
         Usage:\n  {NAME} --version\n  {NAME} identity --json\n  {NAME} serve --state-dir PATH [--capacity NAME=UNITS]\n  {NAME} submit --state-dir PATH [OPTIONS] -- COMMAND...\n  {NAME} snapshot|status|cancel|phase --state-dir PATH [OPTIONS]\n  {NAME} migrate|rollback --state-dir PATH\n\n\
         Commands:\n  identity --json  Report the exact build and durable protocol identity\n  serve            Own and schedule one protocol-5 durable spool\n  submit           Append one validated immutable run\n  snapshot         Read the live queue snapshot\n  status            Read one durable run\n  cancel            Request durable cancellation\n  phase             Commit an identity-verified land phase transition\n  migrate           Explicitly migrate an idle protocol-4 spool\n  rollback          Explicitly return an idle migrated spool to protocol 4"
    );
}

fn identity_json() -> String {
    format!(
        concat!(
            "{{\"name\":\"{}\",",
            "\"version\":\"{}\",",
            "\"protocol\":{},",
            "\"implementation\":\"{}\",",
            "\"build\":\"{}\",",
            "\"target\":\"{}\",",
            "\"sqlite\":\"{}\"}}"
        ),
        NAME,
        VERSION,
        PROTOCOL,
        IMPLEMENTATION,
        BUILD,
        TARGET,
        rusqlite::version()
    )
}

fn option_value(arguments: &[String], index: &mut usize, option: &str) -> Result<String> {
    *index += 1;
    arguments
        .get(*index)
        .cloned()
        .ok_or_else(|| AppError::usage(format!("{option} requires a value")))
}

fn insert_mapping(mapping: &mut BTreeMap<String, u64>, value: &str, subject: &str) -> Result<()> {
    let (name, raw_units) = value
        .split_once('=')
        .ok_or_else(|| AppError::usage(format!("{subject} must use NAME=UNITS")))?;
    if name.is_empty()
        || name.len() > 256
        || name
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte)))
        || mapping.contains_key(name)
    {
        return Err(AppError::usage(format!(
            "{subject} has an invalid or duplicate name"
        )));
    }
    let units = raw_units
        .parse::<u64>()
        .ok()
        .filter(|units| *units > 0 && *units <= i64::MAX as u64)
        .ok_or_else(|| AppError::usage(format!("{subject} units must be a positive integer")))?;
    mapping.insert(name.to_owned(), units);
    Ok(())
}

fn parse_state_only(arguments: &[String]) -> Result<PathBuf> {
    if arguments.len() == 2 && arguments[0] == "--state-dir" {
        return Ok(PathBuf::from(&arguments[1]));
    }
    Err(AppError::usage("command requires exactly --state-dir PATH"))
}

fn parse_run_selector(arguments: &[String], allow_crash: bool) -> Result<(Paths, String, bool)> {
    let mut state_dir = None;
    let mut run_id = None;
    let mut crash_after_commit = false;
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--state-dir" => {
                state_dir = Some(PathBuf::from(option_value(
                    arguments,
                    &mut index,
                    "--state-dir",
                )?));
            }
            "--run-id" => {
                run_id = Some(option_value(arguments, &mut index, "--run-id")?);
            }
            "--crash-after" if allow_crash => {
                if !cfg!(debug_assertions) {
                    return Err(AppError::usage("unknown option: --crash-after"));
                }
                let point = option_value(arguments, &mut index, "--crash-after")?;
                if point != "cancel-commit" {
                    return Err(AppError::usage("unknown cancel crash point"));
                }
                crash_after_commit = true;
            }
            option => return Err(AppError::usage(format!("unknown option: {option}"))),
        }
        index += 1;
    }
    let state_dir = state_dir.ok_or_else(|| AppError::usage("--state-dir is required"))?;
    Ok((
        Paths::new(&state_dir).configured()?,
        run_id.ok_or_else(|| AppError::usage("--run-id is required"))?,
        crash_after_commit,
    ))
}

fn parse_serve(arguments: &[String]) -> Result<ServeOptions> {
    let mut state_dir = None;
    let mut capacities = BTreeMap::new();
    let mut idle_timeout = Duration::from_secs(60);
    let mut crash_after = None;
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--state-dir" => {
                state_dir = Some(PathBuf::from(option_value(
                    arguments,
                    &mut index,
                    "--state-dir",
                )?));
            }
            "--capacity" => {
                let value = option_value(arguments, &mut index, "--capacity")?;
                insert_mapping(&mut capacities, &value, "capacity")?;
            }
            "--idle-timeout" => {
                let value = option_value(arguments, &mut index, "--idle-timeout")?;
                let seconds = value
                    .parse::<f64>()
                    .ok()
                    .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                    .ok_or_else(|| AppError::usage("--idle-timeout must be a positive number"))?;
                idle_timeout = Duration::from_secs_f64(seconds);
            }
            "--crash-after" => {
                if !cfg!(debug_assertions) {
                    return Err(AppError::usage("unknown option: --crash-after"));
                }
                let point = option_value(arguments, &mut index, "--crash-after")?;
                if !matches!(
                    point.as_str(),
                    "owner-lock"
                        | "admission-commit"
                        | "worker-identity-commit"
                        | "terminal-commit"
                        | "worker-cleanup"
                ) {
                    return Err(AppError::usage("unknown broker crash point"));
                }
                crash_after = Some(point);
            }
            option => return Err(AppError::usage(format!("unknown option: {option}"))),
        }
        index += 1;
    }
    if capacities.is_empty() {
        capacities.insert("jobs".to_owned(), 2);
    }
    Ok(ServeOptions {
        state_dir: state_dir.ok_or_else(|| AppError::usage("--state-dir is required"))?,
        capacities,
        idle_timeout,
        crash_after,
    })
}

fn parse_submit(arguments: &[String]) -> Result<(Paths, SubmitRequest)> {
    let separator = arguments
        .iter()
        .position(|argument| argument == "--")
        .ok_or_else(|| AppError::usage("submit requires -- COMMAND..."))?;
    let command = arguments[separator + 1..].to_vec();
    if command.is_empty() {
        return Err(AppError::usage("submit command cannot be empty"));
    }
    let options = &arguments[..separator];
    let mut state_dir = None;
    let mut run_id = None;
    let mut kind = None;
    let mut label = None;
    let mut agent = "unnamed".to_owned();
    let mut repository_id = None;
    let mut repository = None;
    let mut worktree_id = None;
    let mut checkout = None;
    let mut branch = None;
    let mut head_sha = None;
    let mut resources = BTreeMap::new();
    let mut gate_run_id = None;
    let mut environment = BTreeMap::new();
    let mut index = 0;
    while index < options.len() {
        let option = options[index].clone();
        let value = match option.as_str() {
            "--state-dir" | "--run-id" | "--kind" | "--label" | "--agent" | "--repository-id"
            | "--repository" | "--worktree-id" | "--checkout" | "--branch" | "--head"
            | "--resource" | "--gate-run-id" | "--env" => {
                option_value(options, &mut index, &option)?
            }
            option => return Err(AppError::usage(format!("unknown option: {option}"))),
        };
        match option.as_str() {
            "--state-dir" => state_dir = Some(PathBuf::from(value)),
            "--run-id" => run_id = Some(value),
            "--kind" => kind = Some(value),
            "--label" => label = Some(value),
            "--agent" => agent = value,
            "--repository-id" => repository_id = Some(value),
            "--repository" => repository = Some(value),
            "--worktree-id" => worktree_id = Some(value),
            "--checkout" => checkout = Some(PathBuf::from(value)),
            "--branch" => branch = Some(value),
            "--head" => head_sha = Some(value),
            "--gate-run-id" => gate_run_id = Some(value),
            "--resource" => insert_mapping(&mut resources, &value, "resource")?,
            "--env" => {
                let (name, selected) = value
                    .split_once('=')
                    .ok_or_else(|| AppError::usage("environment must use NAME=VALUE"))?;
                if name.is_empty() || name.contains('\0') || environment.contains_key(name) {
                    return Err(AppError::usage(
                        "environment has an empty or duplicate name",
                    ));
                }
                environment.insert(name.to_owned(), selected.to_owned());
            }
            _ => unreachable!(),
        }
        index += 1;
    }
    let state_dir = state_dir.ok_or_else(|| AppError::usage("--state-dir is required"))?;
    Ok((
        Paths::new(&state_dir).configured()?,
        SubmitRequest {
            run_id: run_id.ok_or_else(|| AppError::usage("--run-id is required"))?,
            kind: kind.ok_or_else(|| AppError::usage("--kind is required"))?,
            label: label.ok_or_else(|| AppError::usage("--label is required"))?,
            agent,
            repository_id: repository_id
                .ok_or_else(|| AppError::usage("--repository-id is required"))?,
            repository: repository.ok_or_else(|| AppError::usage("--repository is required"))?,
            worktree_id: worktree_id.ok_or_else(|| AppError::usage("--worktree-id is required"))?,
            checkout: checkout.ok_or_else(|| AppError::usage("--checkout is required"))?,
            branch: branch.ok_or_else(|| AppError::usage("--branch is required"))?,
            head_sha,
            resources,
            gate_run_id,
            command,
            environment,
        },
    ))
}

fn parse_phase(arguments: &[String]) -> Result<(Paths, PhaseRequest)> {
    let mut state_dir = None;
    let mut run_id = None;
    let mut worker_pid = None;
    let mut worker_start_token = None;
    let mut checkout = None;
    let mut head_sha = None;
    let mut phase = None;
    let mut gate_exit_status = None;
    let mut index = 0;
    while index < arguments.len() {
        let option = arguments[index].clone();
        let value = match option.as_str() {
            "--state-dir"
            | "--run-id"
            | "--worker-pid"
            | "--worker-start-token"
            | "--checkout"
            | "--head"
            | "--phase"
            | "--gate-exit-status" => option_value(arguments, &mut index, &option)?,
            option => return Err(AppError::usage(format!("unknown option: {option}"))),
        };
        match option.as_str() {
            "--state-dir" => state_dir = Some(PathBuf::from(value)),
            "--run-id" => run_id = Some(value),
            "--worker-pid" => {
                worker_pid = Some(
                    value
                        .parse::<u32>()
                        .ok()
                        .filter(|pid| *pid > 0)
                        .ok_or_else(|| {
                            AppError::usage("--worker-pid must be a positive integer")
                        })?,
                );
            }
            "--worker-start-token" => worker_start_token = Some(value),
            "--checkout" => checkout = Some(PathBuf::from(value)),
            "--head" => head_sha = Some(value),
            "--phase" => phase = Some(value),
            "--gate-exit-status" => {
                gate_exit_status = Some(
                    value
                        .parse::<i64>()
                        .map_err(|_| AppError::usage("--gate-exit-status must be an integer"))?,
                );
            }
            _ => unreachable!(),
        }
        index += 1;
    }
    let state_dir = state_dir.ok_or_else(|| AppError::usage("--state-dir is required"))?;
    Ok((
        Paths::new(&state_dir).configured()?,
        PhaseRequest {
            run_id: run_id.ok_or_else(|| AppError::usage("--run-id is required"))?,
            worker_pid: worker_pid.ok_or_else(|| AppError::usage("--worker-pid is required"))?,
            worker_start_token: worker_start_token
                .filter(|value| !value.is_empty())
                .ok_or_else(|| AppError::usage("--worker-start-token is required"))?,
            checkout: checkout.ok_or_else(|| AppError::usage("--checkout is required"))?,
            head_sha: head_sha.ok_or_else(|| AppError::usage("--head is required"))?,
            phase: phase.ok_or_else(|| AppError::usage("--phase is required"))?,
            gate_exit_status,
        },
    ))
}

fn emit_json(value: &Value) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string(value).map_err(|error| {
            AppError::new(
                "broker-json-encoding-failed",
                format!("cannot encode command result: {error}"),
            )
        })?
    );
    Ok(())
}

fn run_command(arguments: &[String]) -> Result<()> {
    match arguments {
        [argument] if argument == "--version" || argument == "-V" => {
            println!("{NAME} {VERSION} (protocol {PROTOCOL}, {BUILD})");
            Ok(())
        }
        [command, format] if command == "identity" && format == "--json" => {
            println!("{}", identity_json());
            Ok(())
        }
        [command, rest @ ..] if command == "serve" => Broker::start(parse_serve(rest)?)?.serve(),
        [command, rest @ ..] if command == "submit" => {
            let (paths, request) = parse_submit(rest)?;
            emit_json(&store::submit(&paths, &request)?)
        }
        [command, rest @ ..] if command == "snapshot" => {
            let paths = Paths::new(&parse_state_only(rest)?).configured()?;
            emit_json(&store::snapshot(&paths)?)
        }
        [command, rest @ ..] if command == "status" => {
            let (paths, run_id, _) = parse_run_selector(rest, false)?;
            emit_json(&store::status(&paths, &run_id)?)
        }
        [command, rest @ ..] if command == "cancel" => {
            let (paths, run_id, crash_after) = parse_run_selector(rest, true)?;
            emit_json(&store::cancel(&paths, &run_id, crash_after)?)
        }
        [command, rest @ ..] if command == "phase" => {
            let (paths, request) = parse_phase(rest)?;
            emit_json(&store::advance_land_phase(&paths, &request)?)
        }
        [command, rest @ ..] if command == "migrate" => {
            emit_json(&store::migrate(&parse_state_only(rest)?)?)
        }
        [command, rest @ ..] if command == "rollback" => {
            emit_json(&store::rollback(&parse_state_only(rest)?)?)
        }
        [command] if command == "identity" => Err(AppError::usage("identity requires --json")),
        [unknown, ..] => Err(AppError::usage(format!("unknown command: {unknown}"))),
        [] => {
            print_help();
            Ok(())
        }
    }
}

fn main() -> ExitCode {
    let arguments: Vec<String> = env::args().skip(1).collect();
    if matches!(arguments.as_slice(), [argument] if argument == "--help" || argument == "-h") {
        print_help();
        return ExitCode::SUCCESS;
    }
    match run_command(&arguments) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::to_string(&json!({
                    "code": error.code,
                    "message": error.message,
                }))
                .unwrap()
            );
            ExitCode::from(error.exit_status)
        }
    }
}
