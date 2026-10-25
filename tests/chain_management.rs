use guvna::config::Config;
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{Arc, Barrier},
    thread,
};
use tempfile::TempDir;

fn setup() -> (TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.yaml");
    fs::write(
        &config,
        "providers:\n  - name: p\n    type: openai\n    base_url: https://example.org\n    key_env: KEY\nchains:\n  - name: immutable\n    steps:\n      - provider: p\n        model: static\n",
    )
    .unwrap();
    let data = dir.path().join("data");
    (dir, config, data)
}

fn run(config: &Path, data: &Path, params: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_guvna"))
        .arg("chains")
        .args(params)
        .arg("--config")
        .arg(config)
        .arg("--data")
        .arg(data)
        .output()
        .unwrap()
}

fn success(output: &Output) -> String {
    assert!(
        output.status.success(),
        "exit: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn failure(output: &Output, contains: &str) {
    assert!(!output.status.success(), "unexpected success: {output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(contains),
        "wanted {contains:?} in {stderr:?}"
    );
}

fn definition(path: &Path, name: &str, model: &str) {
    fs::write(
        path,
        format!("name: {name}\nsteps:\n  - provider: p\n    model: {model}\n"),
    )
    .unwrap();
}

#[test]
fn cli_set_replace_delete_and_corrupt_recovery_preserve_every_original_byte() {
    let (dir, config, data) = setup();
    let static_list = success(&run(&config, &data, &["list"]));
    let items: Vec<Value> = serde_json::from_str(&static_list).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "immutable");
    assert_eq!(items[0]["source"], "config");
    let shown: Value = serde_json::from_str(&success(&run(
        &config,
        &data,
        &["show", "--name", "immutable"],
    )))
    .unwrap();
    assert_eq!(shown["name"], "immutable");
    assert_eq!(shown["source"], "config");
    failure(
        &run(&config, &data, &["show", "--name", "missing"]),
        "unknown chain",
    );
    failure(&run(&config, &data, &["show"]), "requires --name");
    assert!(success(&run(&config, &data, &["validate"])).contains("0 runtime"));
    assert!(!data.exists(), "read-only CLI must not create a data dir");

    let definition_file = dir.path().join("chain.yaml");
    definition(&definition_file, "user-route", "model-v1");
    let path = definition_file.to_str().unwrap();
    assert!(
        success(&run(&config, &data, &["set", "--file", path]))
            .contains("created runtime chain user-route")
    );
    let loaded = Config::load(&config)
        .unwrap()
        .with_runtime_chains(&data)
        .unwrap();
    assert_eq!(loaded.chains.len(), 2);
    assert_eq!(loaded.chains[1].steps[0].model, "model-v1");

    // Real second update: backup is byte-for-byte the previous valid file.
    let before_update = fs::read(data.join("chains.yaml")).unwrap();
    definition(&definition_file, "user-route", "model-v2");
    assert!(
        success(&run(&config, &data, &["set", "--file", path]))
            .contains("updated runtime chain user-route")
    );
    assert_eq!(
        fs::read(data.join("chains.yaml.bak")).unwrap(),
        before_update
    );
    let listed: Vec<Value> =
        serde_json::from_str(&success(&run(&config, &data, &["list"]))).unwrap();
    assert_eq!(listed[1]["source"], "runtime");
    assert_eq!(listed[1]["steps"][0]["model"], "model-v2");

    // A base-defined route is not a mutable runtime chain.
    failure(
        &run(&config, &data, &["delete", "--name", "immutable"]),
        "read-only",
    );
    definition(&definition_file, "immutable", "hijack");
    failure(
        &run(&config, &data, &["set", "--file", path]),
        "owned by config.yaml",
    );
    let newest_valid = fs::read(data.join("chains.yaml")).unwrap();

    // Invalid edits can never destroy the most recently committed file.
    fs::write(
        &definition_file,
        "name: broken\nsteps:\n  - provider: unknown\n    model: x\n",
    )
    .unwrap();
    failure(
        &run(&config, &data, &["set", "--file", path]),
        "missing provider",
    );
    assert_eq!(fs::read(data.join("chains.yaml")).unwrap(), newest_valid);

    assert!(
        success(&run(&config, &data, &["delete", "--name", "user-route"]))
            .contains("deleted runtime chain")
    );
    let removed_list: Vec<Value> =
        serde_json::from_str(&success(&run(&config, &data, &["list"]))).unwrap();
    assert_eq!(removed_list.len(), 1);

    // Simulate disk corruption, require explicit recovery, and retain the bad
    // original for forensics rather than overwriting it without a copy.
    let broken = b"version: [\nchains: ???\n";
    fs::write(data.join("chains.yaml"), broken).unwrap();
    failure(&run(&config, &data, &["validate"]), "malformed chains.yaml");
    failure(
        &run(&config, &data, &["set", "--file", path]),
        "missing provider",
    );
    assert_eq!(fs::read(data.join("chains.yaml")).unwrap(), broken);

    let invalid_recovery = dir.path().join("bad-backup.yaml");
    fs::write(&invalid_recovery, "version: [\n").unwrap();
    failure(
        &run(
            &config,
            &data,
            &["recover", "--from", invalid_recovery.to_str().unwrap()],
        ),
        "malformed chains.yaml",
    );
    assert_eq!(fs::read(data.join("chains.yaml")).unwrap(), broken);

    let recovery = success(&run(&config, &data, &["recover"]));
    assert!(
        recovery.contains("recovered 1 runtime chains"),
        "{recovery}"
    );
    let preserved = fs::read_dir(&data)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("chains.yaml.before-recovery-")
        })
        .expect("an explicit forensic copy of corrupt chains.yaml");
    assert_eq!(fs::read(&preserved).unwrap(), broken);
    assert_eq!(fs::read(data.join("chains.yaml")).unwrap(), newest_valid);
    assert!(success(&run(&config, &data, &["validate"])).contains("1 runtime"));

    // Recovered file is usable by the *same* daemon loading path.
    let loaded = Config::load(&config)
        .unwrap()
        .with_runtime_chains(&data)
        .unwrap();
    assert_eq!(loaded.chains[1].steps[0].model, "model-v2");
}

#[test]
fn audit_f08_oversized_corrupt_chain_file_is_recoverable_from_valid_backup() {
    let (_dir, config, data) = setup();
    fs::create_dir_all(&data).unwrap();
    let target = data.join("chains.yaml");
    let invalid = vec![b'!'; 1024 * 1024 + 1];
    fs::write(&target, &invalid).unwrap();
    fs::write(data.join("chains.yaml.bak"), "version: 1\nchains: []\n").unwrap();
    let result = success(&run(&config, &data, &["recover"]));
    assert!(result.contains("recovered 0 runtime chains"), "{result}");
    assert_eq!(
        fs::read_to_string(&target).unwrap(),
        "version: 1\nchains: []\n"
    );
    let preserved = fs::read_dir(&data)
        .unwrap()
        .map(Result::unwrap)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("chains.yaml.before-recovery-")
        })
        .expect("missing forensic copy");
    assert_eq!(fs::read(preserved).unwrap(), invalid);
    assert!(success(&run(&config, &data, &["validate"])).contains("valid:"));
    // A malformed backup must not replace or even move the live file.
    fs::write(data.join("chains.yaml.bak"), "invalid\nyaml\n").unwrap();
    failure(&run(&config, &data, &["recover"]), "guvna:");
    assert_eq!(
        fs::read_to_string(&target).unwrap(),
        "version: 1\nchains: []\n"
    );
}

#[test]
fn concurrent_separate_processes_cannot_lose_runtime_chain_updates() {
    let (dir, config, data) = setup();
    let barrier = Arc::new(Barrier::new(9));
    let mut handles = vec![];
    for i in 0..8 {
        let input = dir.path().join(format!("route-{i}.yaml"));
        definition(&input, &format!("route-{i}"), &format!("model-{i}"));
        let barrier = Arc::clone(&barrier);
        let config = config.clone();
        let data = data.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let input = input.to_string_lossy().to_string();
            let output = run(&config, &data, &["set", "--file", &input]);
            success(&output)
        }));
    }
    barrier.wait();
    for handle in handles {
        assert!(handle.join().unwrap().contains("runtime chain route-"));
    }
    let items: Vec<Value> =
        serde_json::from_str(&success(&run(&config, &data, &["list"]))).unwrap();
    assert_eq!(items.len(), 9, "one static plus all 8 committed routes");
    let runtime = items
        .iter()
        .filter(|entry| entry["source"] == "runtime")
        .collect::<Vec<_>>();
    assert_eq!(runtime.len(), 8);
    for i in 0..8 {
        assert!(runtime.iter().any(|entry| {
            entry["name"] == format!("route-{i}")
                && entry["steps"][0]["model"] == format!("model-{i}")
        }));
    }
    // One atomic rename per commit. No abandoned partial temp files.
    let files = fs::read_dir(&data)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert!(
        files.iter().all(|name| {
            name == "chains.yaml" || name == "chains.yaml.bak" || name == ".chains.yaml.lock"
        }),
        "temporary files leaked: {files:?}"
    );
}

#[cfg(unix)]
#[test]
fn refuses_symlink_target_without_touching_the_target_file() {
    use std::os::unix::fs::symlink;
    let (dir, config, data) = setup();
    fs::create_dir_all(&data).unwrap();
    let outside = dir.path().join("outside.yaml");
    fs::write(&outside, "private-not-for-chains").unwrap();
    symlink(&outside, data.join("chains.yaml")).unwrap();
    let input = dir.path().join("new.yaml");
    definition(&input, "safe", "gpt");
    failure(
        &run(&config, &data, &["set", "--file", input.to_str().unwrap()]),
        "regular file",
    );
    assert_eq!(fs::read(&outside).unwrap(), b"private-not-for-chains");
}

#[test]
fn invalid_runtime_versions_duplicates_and_unknown_fields_fail_without_editing() {
    let (dir, config, data) = setup();
    fs::create_dir_all(&data).unwrap();
    let file = data.join("chains.yaml");
    let input = dir.path().join("valid.yaml");
    definition(&input, "valid", "safe");

    for raw in [
        "version: 2\nchains: []\n",
        "version: 1\nunknown_field: 3\nchains: []\n",
        "version: 1\nchains:\n  - name: repeat\n    steps: [{provider: p, model: x}]\n  - name: repeat\n    steps: [{provider: p, model: y}]\n",
        "version: 1\nchains:\n  - name: immutable\n    steps: [{provider: p, model: override}]\n",
    ] {
        fs::write(&file, raw).unwrap();
        failure(&run(&config, &data, &["validate"]), "guvna:");
        failure(
            &run(&config, &data, &["set", "-file", input.to_str().unwrap()]),
            "guvna:",
        );
        assert_eq!(fs::read_to_string(&file).unwrap(), raw);
    }
}

#[test]
fn file_persistence_preserves_large_integer_parameters_without_float_rounding() {
    let (dir, config, data) = setup();
    let input = dir.path().join("big-int.yaml");
    fs::write(
        &input,
        "name: big-int\nsteps:\n  - provider: p\n    model: gpt\n    params:\n      seed: 12345678901234567890\n      response_format:\n        type: json_object\n",
    )
    .unwrap();
    success(&run(
        &config,
        &data,
        &["set", "-file", input.to_str().unwrap()],
    ));
    let route = Config::load(&config)
        .unwrap()
        .with_runtime_chains(&data)
        .unwrap();
    let params = &route.chains[1].steps[0].params;
    assert_eq!(
        params["seed"].as_u64(),
        Some(12_345_678_901_234_567_890_u64)
    );
    assert_eq!(params["response_format"]["type"], "json_object");
    let persisted = fs::read_to_string(data.join("chains.yaml")).unwrap();
    assert!(persisted.contains("12345678901234567890"), "{persisted}");
}

#[test]
fn auto_reserved_runtime_chain_name_is_rejected_before_write_and_on_load() {
    let (dir, config, data) = setup();
    fs::write(
        &config,
        "providers:\n  - name: p\n    type: openai\n    base_url: https://example.org\n    key_env: KEY\nchains:\n  - name: immutable\n    steps:\n      - provider: p\n        model: static\nauto:\n  enabled: true\n  candidates:\n    - provider: p\n      model: static\n",
    )
    .unwrap();
    let definition_file = dir.path().join("chain.yaml");
    definition(&definition_file, "auto", "shadow");
    failure(
        &run(
            &config,
            &data,
            &["set", "--file", definition_file.to_str().unwrap()],
        ),
        "reserved when Auto is enabled",
    );
    assert!(
        !data.join("chains.yaml").exists(),
        "rejected writes must not create runtime chain state"
    );

    fs::create_dir_all(&data).unwrap();
    let invalid =
        "version: 1\nchains:\n  - name: auto\n    steps: [{provider: p, model: shadow}]\n";
    fs::write(data.join("chains.yaml"), invalid).unwrap();
    let config_value = Config::load(&config).unwrap();
    assert!(
        config_value
            .clone()
            .with_runtime_chains(&data)
            .unwrap_err()
            .contains("reserved when Auto is enabled")
    );
    failure(
        &run(&config, &data, &["validate"]),
        "reserved when Auto is enabled",
    );
    assert_eq!(
        fs::read_to_string(data.join("chains.yaml")).unwrap(),
        invalid
    );
}

#[test]
fn daemon_and_cli_reject_the_same_invalid_runtime_files() {
    let (_tmp, config_file, data) = setup();
    let base = Config::load(&config_file).unwrap();
    fs::create_dir_all(&data).unwrap();
    let live = data.join("chains.yaml");
    let invalid = [
        // Prior daemon silently dropped the shadowed runtime chain.
        (
            "version: 1\nchains:\n  - name: immutable\n    steps: [{provider: p, model: shadow}]\n",
            "shadows config.yaml",
        ),
        // Prior daemon admitted duplicate runtime names instead of rejecting.
        (
            "version: 1\nchains:\n  - name: route\n    steps: [{provider: p, model: a}]\n  - name: route\n    steps: [{provider: p, model: b}]\n",
            "duplicate runtime chain",
        ),
        (
            "version: 1\nchains:\n  - name: route\n    steps: []\n",
            "no steps",
        ),
        (
            "version: 1\nchains:\n  - name: route\n    steps: [{provider: missing, model: x}]\n",
            "missing provider",
        ),
        (
            "version: 1\nchains:\n  - name: route\n    steps: [{provider: p, model: x, params: {messages: hi}}]\n",
            "forbidden param key",
        ),
        (
            "version: 999\nchains: []\n",
            "unsupported chains.yaml version",
        ),
        (
            "version: 1\nunknown: x\nchains: []\n",
            "malformed chains.yaml",
        ),
    ];
    for (yaml, expected) in invalid {
        fs::write(&live, yaml).unwrap();
        let startup = base.clone().with_runtime_chains(&data).unwrap_err();
        assert!(
            startup.contains(expected),
            "daemon startup accepted or misclassified {yaml:?}: {startup}"
        );
        failure(&run(&config_file, &data, &["validate"]), expected);
        // Public binary startup must reject before binding an HTTP port.
        failure(&run_serve_healthcheck(&config_file, &data), expected);
        assert_eq!(fs::read_to_string(&live).unwrap(), yaml);
    }
}

fn run_serve_healthcheck(config: &Path, data: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_guvna"))
        .args(["serve", "-config"])
        .arg(config)
        .arg("-data")
        .arg(data)
        .arg("--healthcheck")
        .output()
        .unwrap()
}

#[test]
fn oversized_runtime_file_fails_consistently_without_being_parsed() {
    let (_tmp, config_file, data) = setup();
    fs::create_dir_all(&data).unwrap();
    let live = data.join("chains.yaml");
    // The former daemon path had no read bound and parsed arbitrary lengths.
    let oversized = [
        b"version: 1\nchains: []\n".as_slice(),
        &vec![b'#'; 1024 * 1024],
    ]
    .concat();
    fs::write(&live, &oversized).unwrap();
    let err = Config::load(&config_file)
        .unwrap()
        .with_runtime_chains(&data)
        .unwrap_err();
    assert!(err.contains("at most 1 MiB"), "{err}");
    failure(&run(&config_file, &data, &["validate"]), "at most 1 MiB");
    failure(&run_serve_healthcheck(&config_file, &data), "at most 1 MiB");
    assert_eq!(fs::metadata(&live).unwrap().len(), oversized.len() as u64);
}

#[cfg(unix)]
#[test]
fn daemon_and_cli_refuse_symlinked_runtime_file_and_lock() {
    use std::os::unix::fs::symlink;
    let (_tmp, config_file, data) = setup();
    fs::create_dir_all(&data).unwrap();
    let original = data.join("original.yaml");
    fs::write(&original, "version: 1\nchains: []\n").unwrap();
    let live = data.join("chains.yaml");
    symlink(&original, &live).unwrap();
    let startup_err = Config::load(&config_file)
        .unwrap()
        .with_runtime_chains(&data)
        .unwrap_err();
    assert!(startup_err.contains("regular file"), "{startup_err}");
    failure(&run(&config_file, &data, &["list"]), "regular file");
    failure(&run_serve_healthcheck(&config_file, &data), "regular file");

    fs::remove_file(&live).unwrap();
    let outside_lock = data.join("outside-lock-file");
    fs::write(&outside_lock, "lock target not overwritten").unwrap();
    symlink(&outside_lock, data.join(".chains.yaml.lock")).unwrap();
    let input = data.join("input.yaml");
    definition(&input, "new", "model");
    failure(
        &run(
            &config_file,
            &data,
            &["set", "-file", input.to_str().unwrap()],
        ),
        "non-regular lock file",
    );
    assert_eq!(
        fs::read_to_string(outside_lock).unwrap(),
        "lock target not overwritten"
    );
}

#[cfg(unix)]
#[test]
fn concurrent_atomic_writer_and_daemon_snapshot_readers_never_parse_partial_yaml() {
    use guvna::chain_store::{self, Command as ChainCommand};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let (dir, config_file, data) = setup();
    let base = Arc::new(Config::load(&config_file).unwrap());
    let a = dir.path().join("a.yaml");
    let b = dir.path().join("b.yaml");
    definition(&a, "changing", "model-A");
    definition(&b, "changing", "model-B");
    let done = Arc::new(AtomicBool::new(false));
    let successes = Arc::new(AtomicUsize::new(0));

    thread::scope(|scope| {
        let writer_base = base.clone();
        let writer_data = data.clone();
        let writer_done = done.clone();
        scope.spawn(move || {
            for i in 0..64 {
                let chosen = if i % 2 == 0 { &a } else { &b };
                let result = chain_store::execute(
                    &writer_base,
                    &writer_data,
                    ChainCommand::Set(chosen.clone()),
                )
                .unwrap();
                assert!(result.contains("runtime chain changing"), "{result}");
            }
            writer_done.store(true, Ordering::Release);
        });
        for _ in 0..6 {
            let base = base.clone();
            let data = data.clone();
            let done = done.clone();
            let successes = successes.clone();
            scope.spawn(move || {
                let mut rounds = 0;
                loop {
                    let loaded = base
                        .as_ref()
                        .clone()
                        .with_runtime_chains(&data)
                        .expect("reader observed partial or invalid YAML");
                    assert!(loaded.chains.len() == 1 || loaded.chains.len() == 2);
                    if loaded.chains.len() == 2 {
                        assert_eq!(loaded.chains[1].name, "changing");
                        assert!(matches!(
                            loaded.chains[1].steps[0].model.as_str(),
                            "model-A" | "model-B"
                        ));
                    }
                    successes.fetch_add(1, Ordering::Relaxed);
                    rounds += 1;
                    if done.load(Ordering::Acquire) && rounds > 50 {
                        break;
                    }
                    std::thread::yield_now();
                }
            });
        }
    });
    assert!(successes.load(Ordering::Relaxed) > 300);
    let final_state = base.as_ref().clone().with_runtime_chains(&data).unwrap();
    assert_eq!(final_state.chains[1].steps[0].model, "model-B");
}

#[test]
fn offline_chain_operations_appear_in_binary_help() {
    let help = Command::new(env!("CARGO_BIN_EXE_guvna"))
        .arg("--help")
        .output()
        .unwrap();
    let output = success(&help);
    for supported in [
        "chains list|validate",
        "chains set",
        "chains delete",
        "chains recover",
        "-config",
        "-data",
    ] {
        assert!(
            output.contains(supported),
            "help omits {supported}: {output}"
        );
    }
}
