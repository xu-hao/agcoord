use std::env;
use std::process::ExitCode;

const NAME: &str = "agcoord-broker";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const PROTOCOL: u32 = 5;
const IMPLEMENTATION: &str = "rust-native";
const BUILD: &str = env!("AGCOORD_BUILD_ID");
const TARGET: &str = env!("AGCOORD_TARGET");

fn print_help() {
    println!(
        "{NAME} — native AGCoord scheduler and enforcement owner\n\n\
         Usage:\n  {NAME} --version\n  {NAME} identity --json\n\n\
         Commands:\n  identity --json  Report the exact build and durable protocol identity"
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

fn usage_error(message: &str) -> ExitCode {
    eprintln!("{NAME}: {message}");
    eprintln!("Try '{NAME} --help' for usage.");
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let arguments: Vec<String> = env::args().skip(1).collect();
    match arguments.as_slice() {
        [] => {
            print_help();
            ExitCode::SUCCESS
        }
        [argument] if argument == "--help" || argument == "-h" => {
            print_help();
            ExitCode::SUCCESS
        }
        [argument] if argument == "--version" || argument == "-V" => {
            println!("{NAME} {VERSION} (protocol {PROTOCOL}, {BUILD})");
            ExitCode::SUCCESS
        }
        [command, format] if command == "identity" && format == "--json" => {
            println!("{}", identity_json());
            ExitCode::SUCCESS
        }
        [command] if command == "identity" => usage_error("identity requires --json"),
        [unknown, ..] => usage_error(&format!("unknown command: {unknown}")),
    }
}
