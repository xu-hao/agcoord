use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const BROKER: &str = env!("CARGO_BIN_EXE_agcoord-broker");

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "agcoord-native-identity-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn output(arguments: &[&str]) -> std::process::Output {
    Command::new(BROKER)
        .args(arguments)
        .output()
        .expect("start agcoord-broker")
}

#[test]
fn reports_exact_version_and_protocol_identity() {
    let version = output(&["--version"]);
    assert!(
        version.status.success(),
        "{}",
        String::from_utf8_lossy(&version.stderr)
    );
    assert_eq!(
        String::from_utf8(version.stdout).unwrap(),
        concat!(
            "agcoord-broker ",
            env!("CARGO_PKG_VERSION"),
            " (protocol 5, development)\n"
        )
    );

    let identity = output(&["identity", "--json"]);
    assert!(
        identity.status.success(),
        "{}",
        String::from_utf8_lossy(&identity.stderr)
    );
    assert_eq!(
        String::from_utf8(identity.stdout).unwrap(),
        format!(
            concat!(
                "{{\"name\":\"agcoord-broker\",",
                "\"version\":\"{}\",",
                "\"protocol\":5,",
                "\"implementation\":\"rust-native\",",
                "\"build\":\"development\",",
                "\"target\":\"{}\",",
                "\"sqlite\":\"{}\"}}\n"
            ),
            env!("CARGO_PKG_VERSION"),
            env!("AGCOORD_TARGET"),
            rusqlite::version()
        )
    );
}

#[test]
fn copied_binary_runs_without_python_or_checkout_files() {
    let directory = TestDirectory::new();
    let copied = directory.path().join("agcoord-broker");
    fs::write(&copied, fs::read(BROKER).unwrap()).unwrap();
    fs::set_permissions(&copied, fs::Permissions::from_mode(0o755)).unwrap();

    let result = Command::new(&copied)
        .args(["identity", "--json"])
        .current_dir(directory.path())
        .env_clear()
        .output()
        .expect("start isolated broker identity command");

    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stdout = String::from_utf8(result.stdout).unwrap();
    assert!(stdout.contains("\"protocol\":5"));
    assert!(stdout.contains("\"implementation\":\"rust-native\""));
}

#[test]
fn internal_worker_mode_is_not_a_public_command() {
    let result = output(&["worker"]);
    assert_eq!(result.status.code(), Some(2));
    assert!(
        String::from_utf8(result.stderr)
            .unwrap()
            .contains("unknown command: worker")
    );
}
