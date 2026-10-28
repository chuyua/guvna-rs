//! Offline, local-only runtime chain management.
//! Mutations are serialized with an OS advisory file lock and committed using
//! a synced temporary file followed by one atomic rename. No management HTTP
//! surface, background reload, secret logging, or in-process mutable routing.
use crate::config::{Chain, Config, RuntimeChains};
use fs2::FileExt;
use serde_json::json;
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const MAX_CHAIN_FILE_BYTES: usize = 1024 * 1024;
// Offline updates fsync the backup, new file and directory while holding an
// OS lock. Allow concurrent local writers to serialize even on slow disks.
const MUTATION_LOCK_WAIT: Duration = Duration::from_secs(5);

pub enum Command {
    List,
    Show(String),
    Validate,
    Set(PathBuf),
    Delete(String),
    Recover(Option<PathBuf>),
}

fn runtime_path(data_dir: &Path) -> PathBuf {
    data_dir.join("chains.yaml")
}

fn read_limited(path: &Path) -> Result<Option<Vec<u8>>, String> {
    // An existing FIFO can block forever in open(2). Reject non-regular
    // nodes before opening, then recheck the opened inode as below.
    match fs::symlink_metadata(path) {
        Ok(meta) if !meta.file_type().is_file() => {
            return Err(format!("{} is not a regular file", path.display()));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("stat {}: {err}", path.display())),
        _ => {}
    }
    // Open the checked inode, not the pathname after a separate metadata
    // check. Linux O_NOFOLLOW closes the symlink swap race between stat/open.
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // A regular file can be replaced with a FIFO after the pathname
        // check. O_NONBLOCK makes that swap fail without hanging the daemon.
        // It has no effect on normal regular-file reads.
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(format!(
                "opening {} (requires a regular file, no symlinks): {err}",
                path.display()
            ));
        }
    };
    let meta = file
        .metadata()
        .map_err(|err| format!("stat open {}: {err}", path.display()))?;
    if !meta.file_type().is_file() || meta.len() > MAX_CHAIN_FILE_BYTES as u64 {
        return Err(format!(
            "{} must be a regular file of at most 1 MiB",
            path.display()
        ));
    }
    // Bound the allocation even if an uncooperative writer extends a file
    // during the read. Metadata length alone is not a sufficient limit.
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    file.take(MAX_CHAIN_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| format!("reading {}: {err}", path.display()))?;
    if bytes.len() > MAX_CHAIN_FILE_BYTES {
        return Err(format!("{} exceeds 1 MiB", path.display()));
    }
    Ok(Some(bytes))
}

fn validate_data(bytes: Option<&[u8]>, config: &Config) -> Result<RuntimeChains, String> {
    let mut state = match bytes {
        Some(bytes) => serde_yaml::from_slice::<RuntimeChains>(bytes)
            .map_err(|e| format!("malformed chains.yaml (preserved): {e}"))?,
        None => RuntimeChains {
            version: Some(1),
            chains: vec![],
        },
    };
    if state.version.unwrap_or(1) != 1 {
        return Err("unsupported chains.yaml version (preserved)".into());
    }
    let mut names = HashSet::new();
    for chain in &state.chains {
        if chain.name.is_empty() || !names.insert(chain.name.as_str()) {
            return Err(format!(
                "empty or duplicate runtime chain {:?} (preserved)",
                chain.name
            ));
        }
        if config.chains.iter().any(|base| base.name == chain.name) {
            return Err(format!(
                "runtime chain {:?} shadows config.yaml (preserved)",
                chain.name
            ));
        }
        if config.auto.enabled && chain.name == "auto" {
            return Err(
                "runtime chain name auto is reserved when Auto is enabled (preserved)".into(),
            );
        }
        config.validate_chain(chain)?;
    }
    state.version = Some(1);
    Ok(state)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_CHAIN_FILE_BYTES {
        return Err("serialized chains.yaml exceeds 1 MiB".into());
    }
    let parent = path.parent().ok_or("chains file has no parent directory")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|e| format!("temporary file in {}: {e}", parent.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("restricting temporary file permissions: {e}"))?;
    }
    temporary
        .write_all(bytes)
        .map_err(|e| format!("writing temporary chains file: {e}"))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|e| format!("syncing temporary chains file: {e}"))?;
    temporary
        .persist(path)
        .map_err(|e| format!("committing {}: {}", path.display(), e.error))?;
    #[cfg(unix)]
    File::open(parent)
        .and_then(|dir| dir.sync_all())
        .map_err(|e| format!("syncing chains directory {}: {e}", parent.display()))?;
    Ok(())
}

fn lock_directory(data_dir: &Path, wait: Duration) -> Result<(PathBuf, File), String> {
    fs::create_dir_all(data_dir)
        .map_err(|e| format!("creating data directory {}: {e}", data_dir.display()))?;
    let directory = fs::canonicalize(data_dir)
        .map_err(|e| format!("resolving data directory {}: {e}", data_dir.display()))?;
    let path = directory.join(".chains.yaml.lock");
    if let Ok(meta) = fs::symlink_metadata(&path)
        && !meta.file_type().is_file()
    {
        return Err(format!("refusing non-regular lock file {}", path.display()));
    }
    let mut options = OpenOptions::new();
    options
        .create(true)
        .truncate(false) // preserve the same inode for advisory locks
        .read(true)
        .write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // O_NONBLOCK also prevents a FIFO swapped into the lock pathname
        // from stalling a writer before the inode type can be verified.
        options
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .mode(0o600);
    }
    let lock = options
        .open(&path)
        .map_err(|e| format!("opening {}: {e}", path.display()))?;
    let metadata = lock.metadata().map_err(|e| format!("lock metadata: {e}"))?;
    if !metadata.is_file() {
        return Err(format!("lock {} must be a regular file", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(format!("lock {} must not have hard links", path.display()));
        }
        if metadata.mode() & 0o022 != 0 {
            return Err(format!(
                "lock {} must not be group/world writable",
                path.display()
            ));
        }
    }
    // A stalled second writer must not block the CLI forever.
    let lock_deadline = Instant::now() + wait;
    loop {
        match lock.try_lock_exclusive() {
            Ok(()) => break,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= lock_deadline {
                    return Err(format!("lock {} is busy", path.display()));
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(err) => return Err(format!("locking {}: {err}", path.display())),
        }
    }
    // The pathname must still identify the locked inode. Otherwise a
    // concurrent unlink/recreate could give two writers different locks.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let current = fs::symlink_metadata(&path)
            .map_err(|e| format!("checking locked file {}: {e}", path.display()))?;
        if !current.file_type().is_file()
            || (current.dev(), current.ino()) != (metadata.dev(), metadata.ino())
        {
            return Err(format!(
                "lock file {} changed while locking",
                path.display()
            ));
        }
    }
    Ok((directory, lock))
}

/// Share one strict runtime format and size contract between the CLI and
/// daemon startup. A malformed file always fails closed; nothing silently
/// shadows or disappears when the process restarts.
pub(crate) fn load_runtime(config: &Config, data_dir: &Path) -> Result<RuntimeChains, String> {
    let path = runtime_path(data_dir);
    let bytes = read_limited(&path)?;
    validate_data(bytes.as_deref(), config)
        .map_err(|e| format!("invalid {} (not modified): {e}", path.display()))
}

fn serialize(state: &RuntimeChains) -> Result<Vec<u8>, String> {
    let serialized =
        serde_yaml::to_string(state).map_err(|e| format!("serializing chains: {e}"))?;
    let bytes = serialized.into_bytes();
    if bytes.len() > MAX_CHAIN_FILE_BYTES {
        return Err("updated chains.yaml would exceed 1 MiB (unchanged)".into());
    }
    Ok(bytes)
}

/// Guard the persistent restart contract before writing a changed chains.yaml.
/// AppState rejects enabled Auto with zero materialized candidates at boot;
/// allowing the last discoverable chain to be deleted would leave a running
/// daemon apparently healthy but unable to restart. Explicit candidates
/// remain valid even when no runtime chains are present.
fn ensure_restartable_auto(config: &Config, runtime: &RuntimeChains) -> Result<(), String> {
    if !config.auto.enabled {
        return Ok(());
    }
    let mut effective = config.clone();
    effective.chains.extend(runtime.chains.iter().cloned());
    effective.discover_auto_candidates();
    if effective.auto.candidates.is_empty() {
        return Err(
            "chain mutation would leave enabled Auto without eligible candidates; chains.yaml unchanged"
                .into(),
        );
    }
    Ok(())
}

fn preserve_original(dir: &Path, source: &Path) -> Result<PathBuf, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock: {e}"))?
        .as_nanos();
    let path = dir.join(format!(
        "chains.yaml.before-recovery-{}-{now}.bak",
        std::process::id()
    ));
    // Recovery is the one operation that must accept an oversized corrupt
    // live file. Stream its bytes to a private file instead of materializing
    // the whole file in RAM or using the 1 MiB validation reader.
    let mut reader = OpenOptions::new();
    reader.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        reader.custom_flags(libc::O_NOFOLLOW);
    }
    let mut original = reader
        .open(source)
        .map_err(|e| format!("opening live file {} for recovery: {e}", source.display()))?;
    if !original
        .metadata()
        .map_err(|e| format!("checking original file: {e}"))?
        .is_file()
    {
        return Err("recovery original must be a regular file".into());
    }
    let mut temp =
        tempfile::NamedTempFile::new_in(dir).map_err(|e| format!("creating forensic copy: {e}"))?;
    std::io::copy(&mut original, temp.as_file_mut())
        .map_err(|e| format!("copying original file: {e}"))?;
    temp.as_file()
        .sync_all()
        .map_err(|e| format!("syncing forensic copy: {e}"))?;
    temp.persist_noclobber(&path)
        .map_err(|e| format!("preserving {}: {}", path.display(), e.error))?;
    #[cfg(unix)]
    File::open(dir)
        .and_then(|parent| parent.sync_all())
        .map_err(|e| {
            format!(
                "syncing preserved original directory {}: {e}",
                dir.display()
            )
        })?;
    Ok(path)
}

/// All management operations are offline. A running daemon sees new routes
/// only after a deliberate restart. Invalid files are not overwritten by
/// Set/Delete; Recover requires an explicitly validated backup.
pub fn execute(config: &Config, data_dir: &Path, command: Command) -> Result<String, String> {
    match command {
        Command::List => {
            let file = runtime_path(data_dir);
            let current = read_limited(&file)?;
            let state = validate_data(current.as_deref(), config)?;
            let static_chains = config
                .chains
                .iter()
                .map(|chain| json!({"source":"config","name":chain.name,"steps":chain.steps}));
            let dynamic_chains = state
                .chains
                .iter()
                .map(|chain| json!({"source":"runtime","name":chain.name,"steps":chain.steps}));
            let listed = static_chains.chain(dynamic_chains).collect::<Vec<_>>();
            serde_json::to_string_pretty(&listed)
                .map_err(|e| format!("encoding chain listing: {e}"))
        }
        Command::Show(name) => {
            // Reuse the same size limit and validation as list; never access an
            // unvalidated chain file or print any provider credentials.
            let list = execute(config, data_dir, Command::List)?;
            let values: Vec<serde_json::Value> = serde_json::from_str(&list)
                .map_err(|_| "cannot decode local chain listing".to_string())?;
            let chosen = values
                .into_iter()
                .find(|chain| {
                    chain.get("name").and_then(serde_json::Value::as_str) == Some(name.as_str())
                })
                .ok_or_else(|| format!("unknown chain {name:?}"))?;
            serde_json::to_string_pretty(&chosen)
                .map_err(|_| "cannot format local chain".to_string())
        }
        Command::Validate => {
            let file = runtime_path(data_dir);
            let current = read_limited(&file)?;
            let runtime = validate_data(current.as_deref(), config)?;
            Ok(format!(
                "valid: {} static, {} runtime chains; file={}",
                config.chains.len(),
                runtime.chains.len(),
                file.display()
            ))
        }
        Command::Set(source) => {
            let input = read_limited(&source)?
                .ok_or_else(|| format!("missing chain input {}", source.display()))?;
            let chain: Chain = serde_yaml::from_slice(&input)
                .map_err(|e| format!("invalid chain definition {}: {e}", source.display()))?;
            if chain.name.is_empty() || config.chains.iter().any(|c| c.name == chain.name) {
                return Err("chain name is empty or owned by config.yaml".into());
            }
            if config.auto.enabled && chain.name == "auto" {
                return Err("chain name auto is reserved when Auto is enabled".into());
            }
            config.validate_chain(&chain)?;
            let (directory, _lock) = lock_directory(data_dir, MUTATION_LOCK_WAIT)?;
            let target = runtime_path(&directory);
            let original = read_limited(&target)?;
            let mut state = validate_data(original.as_deref(), config)?;
            let name = chain.name.clone();
            let replacing = if let Some(old) = state.chains.iter_mut().find(|c| c.name == name) {
                *old = chain;
                true
            } else {
                state.chains.push(chain);
                false
            };
            ensure_restartable_auto(config, &state)?;
            let next = serialize(&state)?;
            if let Some(bytes) = &original {
                atomic_write(&directory.join("chains.yaml.bak"), bytes)?;
            }
            atomic_write(&target, &next)?;
            Ok(format!(
                "{} runtime chain {name}; restart guvna to apply",
                if replacing { "updated" } else { "created" }
            ))
        }
        Command::Delete(name) => {
            if name.is_empty() {
                return Err("delete requires a nonempty chain name".into());
            }
            let (directory, _lock) = lock_directory(data_dir, MUTATION_LOCK_WAIT)?;
            let target = runtime_path(&directory);
            let original = read_limited(&target)?;
            let mut state = validate_data(original.as_deref(), config)?;
            let count = state.chains.len();
            state.chains.retain(|chain| chain.name != name);
            if state.chains.len() == count {
                return Err(format!(
                    "runtime chain {name:?} not found; config.yaml chains are read-only"
                ));
            }
            ensure_restartable_auto(config, &state)?;
            let next = serialize(&state)?;
            if let Some(bytes) = &original {
                atomic_write(&directory.join("chains.yaml.bak"), bytes)?;
            }
            atomic_write(&target, &next)?;
            Ok(format!(
                "deleted runtime chain {name}; restart guvna to apply"
            ))
        }
        Command::Recover(source) => {
            let (directory, _lock) = lock_directory(data_dir, MUTATION_LOCK_WAIT)?;
            let target = runtime_path(&directory);
            let backup = source.unwrap_or_else(|| directory.join("chains.yaml.bak"));
            let incoming = read_limited(&backup)?
                .ok_or_else(|| format!("recovery source {} not found", backup.display()))?;
            let validated = validate_data(Some(&incoming), config)?;
            ensure_restartable_auto(config, &validated)?;
            let next = serialize(&validated)?;
            // Preserve even a corrupt original byte-for-byte and report its
            // name. Never rewrite a failed recovery source or the live file.
            let preserved = match fs::symlink_metadata(&target) {
                Ok(meta) if meta.file_type().is_file() => {
                    Some(preserve_original(&directory, &target)?)
                }
                Ok(_) => return Err("recovery live file is not a regular file".into()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => return Err(format!("checking live file before recovery: {err}")),
            };
            atomic_write(&target, &next)?;
            Ok(format!(
                "recovered {} runtime chains from {}; prior content={}; restart guvna to apply",
                validated.chains.len(),
                backup.display(),
                preserved.map_or_else(|| "none".into(), |p| p.display().to_string())
            ))
        }
    }
}

#[cfg(all(test, unix))]
mod lock_security_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn fifo_is_rejected_without_waiting_for_a_writer() {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("chains.yaml");
        let name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        assert!(
            read_limited(&fifo)
                .unwrap_err()
                .contains("not a regular file")
        );
        // Simulate the result of an attacker replacing a regular file with
        // this FIFO between lstat and open. The open must not block.
        let mut opts = OpenOptions::new();
        use std::os::unix::fs::OpenOptionsExt;
        opts.read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        let opened = opts.open(&fifo).unwrap();
        assert!(!opened.metadata().unwrap().file_type().is_file());
        let lock = dir.path().join(".chains.yaml.lock");
        let lock_name = CString::new(lock.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(lock_name.as_ptr(), 0o600) }, 0);
        assert!(
            lock_directory(dir.path(), Duration::from_millis(750))
                .unwrap_err()
                .contains("non-regular")
        );
        let mut opts = OpenOptions::new();
        opts.read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        let opened = opts.open(&lock).unwrap();
        assert!(!opened.metadata().unwrap().file_type().is_file());
    }

    #[test]
    fn shared_writable_lock_file_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".chains.yaml.lock");
        fs::write(&path, b"").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(lock_directory(dir.path(), Duration::from_millis(750)).is_err());
    }

    #[test]
    fn ordinary_private_lock_remains_usable_across_sequential_writers() {
        let dir = tempfile::tempdir().unwrap();
        let (first_directory, first) =
            lock_directory(dir.path(), Duration::from_millis(750)).unwrap();
        assert_eq!(first_directory, fs::canonicalize(dir.path()).unwrap());
        drop(first);
        let (second_directory, second) =
            lock_directory(dir.path(), Duration::from_millis(750)).unwrap();
        assert_eq!(first_directory, second_directory);
        drop(second);
    }

    #[test]
    fn held_chain_lock_fails_bounded_then_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let (_, held) = lock_directory(dir.path(), Duration::from_millis(750)).unwrap();
        let start = Instant::now();
        let failure = lock_directory(dir.path(), Duration::from_millis(750)).unwrap_err();
        assert!(failure.contains("busy"), "{failure}");
        assert!(start.elapsed() < Duration::from_secs(2));
        drop(held);
        assert!(lock_directory(dir.path(), Duration::from_millis(750)).is_ok());
    }

    #[test]
    fn chain_mutation_waits_for_fsync_contention_before_failing() {
        let dir = tempfile::tempdir().unwrap();
        let (_, held) = lock_directory(dir.path(), Duration::from_millis(750)).unwrap();
        let contender_dir = dir.path().to_path_buf();
        let contender = thread::spawn(move || {
            lock_directory(&contender_dir, MUTATION_LOCK_WAIT).map(|(_, lock)| drop(lock))
        });
        // A second offline writer must not fail just because the first still
        // has a legitimate backup and directory fsync in progress.
        thread::sleep(Duration::from_millis(1000));
        drop(held);
        assert!(
            contender.join().unwrap().is_ok(),
            "a local mutation should wait for a legitimate writer to finish"
        );
    }

    #[test]
    fn hardlinked_lock_file_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".chains.yaml.lock");
        fs::write(&path, b"").unwrap();
        fs::hard_link(&path, dir.path().join("another-link")).unwrap();
        assert!(lock_directory(dir.path(), Duration::from_millis(750)).is_err());
    }
}
