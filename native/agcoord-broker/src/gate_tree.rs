//! Reusable exact-head checkouts owned by one spool. All Git and disk walks run in
//! the forked launcher, never in the scheduler. The setup lock serializes eviction.
use crate::cgroup::sha256_prefix;
use crate::error::{AppError, Result};
use crate::platform::prepare_private_directory;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const MAX_TREES: usize = 16;
const MAX_BYTES: u64 = 20 * 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct GateTree {
    pub source: PathBuf,
    pub checkout: PathBuf,
    pub branch: String,
    pub head: String,
}

fn failure(message: impl Into<String>) -> AppError {
    AppError::new("gate-tree-setup-failed", message)
}

fn git(path: &Path, arguments: &[&str]) -> Result<String> {
    // Use a fixed helper and no inherited Git routing/configuration environment. Hooks
    // and fsmonitor must never execute submitted code in the setup domain.
    let output = Command::new(crate::host::GATE_GIT_EXECUTABLE)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "submodule.recurse=false",
            "-C",
        ])
        .arg(path)
        .args(arguments)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| failure(format!("cannot execute Git: {error}")))?;
    if !output.status.success() {
        return Err(failure(String::from_utf8_lossy(&output.stderr).trim()));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn disk_bytes(path: &Path) -> Result<u64> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(failure(error.to_string())),
    };
    let mut bytes = metadata.blocks().saturating_mul(512);
    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|error| failure(error.to_string()))? {
            bytes = bytes.saturating_add(disk_bytes(
                &entry.map_err(|error| failure(error.to_string()))?.path(),
            )?);
        }
    }
    Ok(bytes)
}

impl GateTree {
    pub fn path(state: &Path, repository: &str, branch: &str) -> PathBuf {
        let key = sha256_prefix(format!("{repository}\0{branch}").as_bytes(), 32);
        state.join("gate-trees").join(key).join("checkout")
    }

    pub fn prepare(&self) -> Result<()> {
        self.prepare_bounded(MAX_TREES, MAX_BYTES).map_err(|error| {
            if error.code.starts_with("gate-tree-") {
                error
            } else {
                failure(error.message)
            }
        })
    }

    fn prepare_bounded(&self, max_trees: usize, max_bytes: u64) -> Result<()> {
        if !self.source.is_dir() {
            return Err(AppError::new(
                "gate-tree-checkout-missing",
                "submitted checkout no longer exists",
            ));
        }
        if git(
            &self.source,
            &["cat-file", "-e", &format!("{}^{{commit}}", self.head)],
        )
        .is_err()
        {
            return Err(AppError::new(
                "gate-tree-head-missing",
                "submitted head is not present in the source repository",
            ));
        }
        let common = git(
            &self.source,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )?;
        let directory = self.checkout.parent().unwrap();
        let root = directory.parent().unwrap();
        prepare_private_directory(root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(root.join("setup.lock"))
            .map_err(|error| failure(error.to_string()))?;
        // This wait is in the cancellable launcher's process group. No scheduler lock
        // or database transaction is held while waiting or doing filesystem work.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(failure("cannot lock gate-tree setup"));
        }
        let state = root.parent().unwrap();
        let connection = crate::store::connect(&crate::store::Paths::new(state))?;
        let protected: Vec<_> = crate::store::load_live_runs(&connection)?
            .into_iter()
            .filter(|run| run.status == "running" && matches!(run.kind.as_str(), "full" | "land"))
            .map(|run| Self::path(state, &run.repository_id, &run.branch))
            .collect();
        drop(connection);
        let mut candidates = Vec::new();
        for entry in fs::read_dir(root).map_err(|error| failure(error.to_string()))? {
            let path = entry.map_err(|error| failure(error.to_string()))?.path();
            if !fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.is_dir()) {
                continue;
            }
            let tree = path.join("checkout");
            let metadata_path = path.join("owner.json");
            if !metadata_path.exists() && !tree.exists() {
                // A launcher may have died between mkdir and writing its ownership.
                fs::remove_dir_all(&path).map_err(|error| failure(error.to_string()))?;
                continue;
            }
            let record: Value = serde_json::from_slice(
                &fs::read(&metadata_path).map_err(|error| failure(error.to_string()))?,
            )
            .map_err(|error| failure(error.to_string()))?;
            let repository = Path::new(
                record["common"]
                    .as_str()
                    .ok_or_else(|| failure("invalid gate-tree owner"))?,
            );
            let branch = record["branch"]
                .as_str()
                .ok_or_else(|| failure("invalid gate-tree branch"))?;
            let protected = tree == self.checkout || protected.contains(&tree);
            let gone = !repository.is_dir()
                || git(
                    repository,
                    &["show-ref", "--verify", &format!("refs/heads/{branch}")],
                )
                .is_err();
            if gone && !protected {
                Self::remove(&path, repository)?;
                continue;
            }
            let size = disk_bytes(&path)?;
            let age = fs::metadata(metadata_path)
                .and_then(|metadata| metadata.modified())
                .map_err(|error| failure(error.to_string()))?;
            candidates.push((age, path, repository.to_owned(), size, protected));
        }
        candidates.sort_by_key(|candidate| candidate.0);
        let exists = directory.exists();
        let mut count = candidates.len() + usize::from(!exists);
        let mut bytes: u64 = candidates.iter().map(|candidate| candidate.3).sum();
        for (_, path, repository, size, protected) in &candidates {
            if count <= max_trees && bytes < max_bytes {
                break;
            }
            if !protected {
                Self::remove(path, repository)?;
                count -= 1;
                bytes = bytes.saturating_sub(*size);
            }
        }
        if count > max_trees || bytes >= max_bytes {
            return Err(AppError::new(
                "gate-tree-capacity",
                "gate-tree cache is full and no idle tree can be evicted",
            ));
        }
        // Repository identities may join independent clones of the same origin.
        // Keep the existing tree warm and import only the requested objects from the
        // current source clone; neither clone's local branch is advanced.
        let mut gate_common = common.clone();
        if self.checkout.join(".git").is_file() {
            gate_common = git(
                &self.checkout,
                &["rev-parse", "--path-format=absolute", "--git-common-dir"],
            )?;
            if gate_common != common {
                git(
                    &self.checkout,
                    &[
                        "fetch",
                        "--no-tags",
                        "--no-write-fetch-head",
                        self.source.to_str().unwrap(),
                        &self.head,
                    ],
                )?;
            }
        }
        prepare_private_directory(directory)?;
        // Write ownership first so an interrupted add is discoverable and repairable.
        fs::write(
            directory.join("owner.json.tmp"),
            serde_json::to_vec(&json!({"common": gate_common, "branch": self.branch})).unwrap(),
        )
        .map_err(|error| failure(error.to_string()))?;
        fs::rename(
            directory.join("owner.json.tmp"),
            directory.join("owner.json"),
        )
        .map_err(|error| failure(error.to_string()))?;
        if !self.checkout.join(".git").is_file() {
            // Only this spool's exact path is pruned, never another contributor's tree.
            let _ = git(
                &self.source,
                &[
                    "worktree",
                    "remove",
                    "--force",
                    "--force",
                    self.checkout.to_str().unwrap(),
                ],
            );
            if self.checkout.exists() {
                fs::remove_dir_all(&self.checkout).map_err(|error| failure(error.to_string()))?;
            }
            git(
                &self.source,
                &[
                    "worktree",
                    "add",
                    "--detach",
                    self.checkout.to_str().unwrap(),
                    &self.head,
                ],
            )?;
        }
        // A previous command could have attached HEAD. Detach without touching the
        // index or files before reset can accidentally advance a shared local ref.
        // Admission follows complete process-group drainage of the previous owner,
        // so only this launcher's Git can be using these tree-specific lock files.
        // A SIGKILL during an earlier reset/add may have left them behind.
        for name in ["index.lock", "HEAD.lock"] {
            let lock = git(
                &self.checkout,
                &["rev-parse", "--path-format=absolute", "--git-path", name],
            )?;
            match fs::remove_file(lock) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(failure(error.to_string())),
            }
        }
        git(
            &self.checkout,
            &["update-ref", "--no-deref", "HEAD", &self.head],
        )?;
        git(&self.checkout, &["reset", "--hard", &self.head])?;
        // Reset leaves ignored caches warm. Remove other untracked gate outputs so a
        // previous command cannot contaminate the next exact-head verdict.
        git(&self.checkout, &["clean", "-ffd"])?;
        let total = disk_bytes(root)?;
        if total > max_bytes {
            Self::remove(directory, Path::new(&gate_common))?;
            return Err(AppError::new(
                "gate-tree-capacity",
                "materialized gate tree exceeds the cache disk budget",
            ));
        }
        Ok(())
    }

    fn remove(directory: &Path, common: &Path) -> Result<()> {
        let tree = directory.join("checkout");
        if common.is_dir() {
            let registered = git(common, &["worktree", "list", "--porcelain"])?;
            if registered
                .lines()
                .any(|line| line.strip_prefix("worktree ") == tree.to_str())
            {
                git(
                    common,
                    &[
                        "worktree",
                        "remove",
                        "--force",
                        "--force",
                        tree.to_str().unwrap(),
                    ],
                )?;
            }
        }
        fs::remove_dir_all(directory).map_err(|error| failure(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn disk_budget_refuses_materialization_without_touching_source() {
        let root = Directory(std::env::temp_dir().join(format!(
                "agcoord-gate-budget-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )));
        let source = root.0.join("source");
        fs::create_dir_all(&source).unwrap();
        for args in [
            vec!["init", "-q", "-b", "ticket"],
            vec![
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-q",
                "--allow-empty",
                "-m",
                "initial",
            ],
        ] {
            git(&source, &args).unwrap();
        }
        let head = git(&source, &["rev-parse", "HEAD"]).unwrap();
        let state = root.0.join("state");
        let _connection = crate::store::initialize_native(
            &crate::store::Paths::new(&state),
            &BTreeMap::from([("jobs".to_owned(), 1)]),
        )
        .unwrap();
        let tree = GateTree {
            source: source.clone(),
            checkout: GateTree::path(&state, "repo", "ticket"),
            branch: "ticket".into(),
            head: head.clone(),
        };
        let error = tree.prepare_bounded(16, 1).unwrap_err();
        assert_eq!(error.code, "gate-tree-capacity");
        assert!(!tree.checkout.exists());
        assert_eq!(git(&source, &["rev-parse", "HEAD"]).unwrap(), head);
        assert!(
            !git(&source, &["worktree", "list", "--porcelain"])
                .unwrap()
                .contains(tree.checkout.to_str().unwrap())
        );
    }
}
