#[cfg(unix)]
mod control_socket;
use guvna::{chain_store, config::Config, gateway};
use std::{
    collections::HashMap,
    env,
    future::{Future, IntoFuture},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("guvna: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    // Set before starting the SQLite worker or binding a Unix control
    // socket. The socket's bind->chmod interval and lazily created WAL/SHM
    // must never inherit permissive service-manager umask settings.
    #[cfg(unix)]
    unsafe {
        libc::umask(0o077);
    }
    let mut config_path = PathBuf::from("config.yaml");
    let mut data_dir = env::var_os("GUVNA_DATA_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("HOME")
                .or_else(|| env::var_os("USERPROFILE"))
                .map(|p| PathBuf::from(p).join(".guvna"))
        })
        .unwrap_or_else(|| PathBuf::from(".guvna"));
    let mut healthcheck = false;
    let mut status = false;
    let mut status_scoring = false;
    let mut status_json = false;
    let mut scoring_window_explicit = false;
    let mut scoring_hours: u32 = 24;
    let mut logs = false;
    let mut system_journal = false;
    let mut tail_specified = false;
    let mut tail: usize = 100;
    let mut chains_action: Option<String> = None;
    let mut chains_file: Option<PathBuf> = None;
    let mut chains_name: Option<String> = None;
    let mut recover_from: Option<PathBuf> = None;
    let mut args = env::args().skip(1).peekable();
    if args.peek().is_some_and(|s| s == "chains") {
        args.next();
        chains_action = Some(
            args.next()
                .ok_or("chains requires list|show|validate|set|delete|recover")?,
        );
    } else if args.peek().is_some_and(|s| s == "logs") {
        args.next();
        logs = true;
    } else if args.peek().is_some_and(|s| s == "status") {
        args.next();
        status = true;
    } else if args.peek().is_some_and(|s| s == "serve") {
        args.next();
    }
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-config" | "--config" => {
                config_path = PathBuf::from(args.next().ok_or("missing -config value")?);
            }
            "-data" | "--data" => {
                data_dir = PathBuf::from(args.next().ok_or("missing -data value")?);
            }
            "--scoring" if status => status_scoring = true,
            "--json" if status => status_json = true,
            "--window" if status => {
                scoring_window_explicit = true;
                scoring_hours = match args.next().as_deref() {
                    Some("1h") => 1,
                    Some("6h") => 6,
                    Some("24h") => 24,
                    Some("7d") => 168,
                    _ => return Err("status --window requires 1h|6h|24h|7d".into()),
                };
            }
            "--system" if logs => system_journal = true,
            "--tail" if logs => {
                tail_specified = true;
                tail = args
                    .next()
                    .ok_or("missing --tail value")?
                    .parse()
                    .map_err(|_| "invalid --tail value")?;
                if !(1..=1000).contains(&tail) {
                    return Err("--tail must be 1..=1000".into());
                }
            }
            "-healthcheck" | "--healthcheck" => healthcheck = true,
            "-file" | "--file" if chains_action.is_some() => {
                chains_file = Some(PathBuf::from(args.next().ok_or("missing --file value")?));
            }
            "-name" | "--name" if chains_action.is_some() => {
                chains_name = Some(args.next().ok_or("missing --name value")?);
            }
            "-from" | "--from" if chains_action.is_some() => {
                recover_from = Some(PathBuf::from(args.next().ok_or("missing --from value")?));
            }
            "--version" | "-version" => {
                println!("guvna {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "--help" | "-h" | "help" => {
                println!(
                    "guvna serve [-config config.yaml] [-data DIR]\nguvna status [--json] [--scoring] [--window 1h|6h|24h|7d] [-config config.yaml]\nguvna logs [--tail 100] [--system]\n  --healthcheck probes /healthz\nguvna chains show -name NAME\nguvna chains list|validate [-config FILE] [-data DIR]\nguvna chains set -file CHAIN.yaml [-config FILE] [-data DIR]\nguvna chains delete -name NAME [-config FILE] [-data DIR]\nguvna chains recover [-from BACKUP] [-config FILE] [-data DIR]\n  --version"
                );
                return Ok(());
            }
            other => return Err(format!("unknown or not-yet-migrated argument {other:?}")),
        }
    }

    validate_status_options(status_scoring, scoring_window_explicit)?;

    if logs {
        #[cfg(not(target_os = "linux"))]
        let _ = system_journal;
        #[cfg(target_os = "linux")]
        {
            let mut command = std::process::Command::new("journalctl");
            if !system_journal {
                command.arg("--user");
            }
            let result = command
                .args(["-u", "guvna.service", "--no-pager", "-n", &tail.to_string()])
                .status()
                .map_err(|e| format!("journalctl unavailable: {e}"))?;
            if !result.success() {
                return Err(format!("journalctl exited: {result}"));
            }
            return Ok(());
        }
        #[cfg(not(target_os = "linux"))]
        return Err("logs requires Linux systemd journal".into());
    }
    if system_journal || tail_specified {
        return Err("journal options require logs subcommand".into());
    }
    if status {
        #[cfg(unix)]
        {
            let snapshot = if status_scoring {
                control_socket::query_scoring(&data_dir, scoring_hours).await?
            } else {
                control_socket::query(&data_dir).await?
            };
            if let Some(snapshot) = snapshot {
                println!("{snapshot}");
                return Ok(());
            }
        }
        if status_scoring {
            return Err("status --scoring requires a running local Unix control socket".into());
        }
    }
    let real_path = std::fs::canonicalize(&config_path)
        .map_err(|e| format!("config path {}: {e}", config_path.display()))?;
    if let Some(action) = chains_action {
        if healthcheck || status {
            return Err("healthcheck/status cannot be combined with chains".into());
        }
        let command = match action.as_str() {
            "list" if chains_file.is_none() && chains_name.is_none() && recover_from.is_none() => {
                chain_store::Command::List
            }
            "show" if chains_file.is_none() && recover_from.is_none() => {
                chain_store::Command::Show(chains_name.ok_or("chains show requires --name NAME")?)
            }
            "validate"
                if chains_file.is_none() && chains_name.is_none() && recover_from.is_none() =>
            {
                chain_store::Command::Validate
            }
            "set" if chains_name.is_none() && recover_from.is_none() => {
                chain_store::Command::Set(chains_file.ok_or("chains set requires --file PATH")?)
            }
            "delete" if chains_file.is_none() && recover_from.is_none() => {
                chain_store::Command::Delete(
                    chains_name.ok_or("chains delete requires --name NAME")?,
                )
            }
            "recover" if chains_file.is_none() && chains_name.is_none() => {
                chain_store::Command::Recover(recover_from)
            }
            _ => return Err("invalid chains subcommand or incompatible flags".into()),
        };
        #[cfg(unix)]
        {
            let online = match &command {
                chain_store::Command::Set(file) => {
                    let absolute = std::fs::canonicalize(file)
                        .map_err(|e| format!("chain source file: {e}"))?;
                    Some(("chains_set", absolute.to_string_lossy().into_owned()))
                }
                chain_store::Command::Delete(name) => Some(("chains_delete", name.clone())),
                _ => None,
            };
            if let Some((action, argument)) = online
                && let Some(result) =
                    control_socket::query_chain_mutation(&data_dir, action, &argument).await?
            {
                println!("{result}");
                return Ok(());
            }
        }
        let config = Config::load(&real_path)?;
        println!("{}", chain_store::execute(&config, &data_dir, command)?);
        eprintln!("guvna: offline chain update; restart daemon to apply runtime routing changes");
        return Ok(());
    }
    let base_config = Config::load(&real_path)?;
    let config = base_config.clone().with_runtime_chains(&data_dir)?;
    if healthcheck || status {
        let bind = env::var("GUVNA_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
        let probe_host = local_probe_host(&bind)?;
        let url = format!("http://{probe_host}:{}/healthz", config.port);
        let response = reqwest::Client::new()
            .get(url)
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await
            .map_err(|e| format!("healthcheck: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("healthcheck HTTP {}", response.status()));
        }
        if status {
            println!(
                "{}",
                status_probe_output(probe_host, config.port, status_json)
            );
        }
        return Ok(());
    }
    let mut client_keys: Vec<String> = env::var("API_KEYS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(String::from)
        .collect();
    if let Ok(admin) = env::var("ADMIN_KEY")
        && !admin.is_empty()
    {
        client_keys.push(admin);
    }
    let credentials: HashMap<String, String> = config
        .providers
        .iter()
        .flat_map(|p| p.key_names())
        .filter_map(|key| env::var(&key).ok().map(|value| (key, value)))
        .collect();
    let port = config.port;
    let mut state = gateway::AppState::new(config, credentials, client_keys)?;
    let telemetry_enabled = env::var("GUVNA_TELEMETRY_SQLITE")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        });
    if telemetry_enabled && state.enable_telemetry(&data_dir).is_err() {
        // Optional local telemetry must never prevent the gateway booting.
        guvna::ops_log::warn("telemetry_init_failed");
        state.record_telemetry_start_failure();
    }
    let bind = env::var("GUVNA_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
    let listener = tokio::net::TcpListener::bind((bind.as_str(), port))
        .await
        .map_err(|e| format!("binding {bind}:{port}: {e}"))?;
    eprintln!(
        "guvna-rs starting; config={}; listening={}",
        real_path.display(),
        listener.local_addr().map_err(|e| e.to_string())?
    );
    let (app, runtime_state) = gateway::app_with_state(state);
    #[cfg(unix)]
    let _control_server =
        control_socket::spawn(&data_dir, Arc::clone(&runtime_state), Arc::new(base_config)).await?;
    serve_until_shutdown(listener, app, shutdown_signal(), Duration::from_secs(30)).await?;
    if !runtime_state.flush_telemetry(Duration::from_secs(3)) {
        guvna::ops_log::warn("telemetry_shutdown_flush_failed");
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        } else {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn serve_until_shutdown(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    shutdown: impl Future<Output = ()>,
    drain: Duration,
) -> Result<(), String> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = rx.await;
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result.map_err(|e| format!("serving: {e}")),
        () = shutdown => {
            eprintln!("guvna-rs shutting down; draining in-flight requests");
            let _ = tx.send(());
            match tokio::time::timeout(drain, &mut server).await {
                Ok(result) => result.map_err(|e| format!("serving during shutdown: {e}")),
                Err(_) => {
                    // Dropping the server future bounds stop time even when a
                    // remote upstream keeps an SSE body open indefinitely.
                    eprintln!("guvna-rs shutdown drain expired; closing remaining connections");
                    Ok(())
                }
            }
        }
    }
}

fn validate_status_options(scoring: bool, window_specified: bool) -> Result<(), String> {
    if window_specified && !scoring {
        return Err("status --window requires --scoring".into());
    }
    Ok(())
}

fn status_probe_output(host: &str, port: u16, json_output: bool) -> String {
    if json_output {
        serde_json::json!({
            "status": "running",
            "source": "local_health_probe",
            "host": host,
            "port": port,
        })
        .to_string()
    } else {
        format!("guvna running on {host}:{port}")
    }
}

fn local_probe_host(bind: &str) -> Result<&str, String> {
    match bind {
        "0.0.0.0" => Ok("127.0.0.1"),
        "::" | "[::]" => Ok("[::1]"),
        "localhost" | "127.0.0.1" | "[::1]" | "::1" => {
            if bind == "::1" {
                Ok("[::1]")
            } else {
                Ok(bind)
            }
        }
        _ => Err("status/healthcheck requires a loopback or wildcard GUVNA_BIND_ADDR".into()),
    }
}

#[cfg(test)]
mod cli_tests {
    use super::{
        local_probe_host, serve_until_shutdown, status_probe_output, validate_status_options,
    };
    use std::{sync::Arc, time::Duration};
    use tokio::sync::{Notify, oneshot};

    #[tokio::test]
    async fn graceful_shutdown_drains_started_requests() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let router = axum::Router::new().route(
            "/slow",
            axum::routing::get({
                let entered = entered.clone();
                let release = release.clone();
                move || {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        "completed before shutdown"
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_until_shutdown(
                listener,
                router,
                async {
                    let _ = shutdown_rx.await;
                },
                Duration::from_secs(3),
            )
            .await
        });
        let request = tokio::spawn(async move {
            reqwest::get(format!("http://{address}/slow"))
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        });
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        shutdown_tx.send(()).unwrap();
        tokio::task::yield_now().await;
        release.notify_one();
        assert_eq!(request.await.unwrap(), "completed before shutdown");
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_has_hard_deadline_for_stalled_request() {
        let entered = Arc::new(Notify::new());
        let router = axum::Router::new().route(
            "/forever",
            axum::routing::get({
                let entered = entered.clone();
                move || {
                    let entered = entered.clone();
                    async move {
                        entered.notify_one();
                        std::future::pending::<&'static str>().await
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_until_shutdown(
                listener,
                router,
                async {
                    let _ = shutdown_rx.await;
                },
                Duration::from_millis(100),
            )
            .await
        });
        let request = tokio::spawn(async move {
            let _ = reqwest::get(format!("http://{address}/forever")).await;
        });
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("shutdown must not wait on a stuck request")
            .unwrap()
            .unwrap();
        request.abort();
    }

    #[test]
    fn status_json_fallback_is_machine_readable_and_window_requires_scoring() {
        assert!(validate_status_options(true, true).is_ok());
        assert!(validate_status_options(false, false).is_ok());
        assert_eq!(
            validate_status_options(false, true).unwrap_err(),
            "status --window requires --scoring"
        );
        let text = status_probe_output("127.0.0.1", 8800, false);
        assert_eq!(text, "guvna running on 127.0.0.1:8800");
        let json: serde_json::Value =
            serde_json::from_str(&status_probe_output("[::1]", 8800, true)).unwrap();
        assert_eq!(json["status"], "running");
        assert_eq!(json["source"], "local_health_probe");
        assert_eq!(json["host"], "[::1]");
        assert_eq!(json["port"], 8800);
        assert!(!json.to_string().contains("API_KEYS"));
    }

    #[test]
    fn healthcheck_never_probes_an_untrusted_remote_host() {
        assert_eq!(local_probe_host("0.0.0.0").unwrap(), "127.0.0.1");
        assert_eq!(local_probe_host("::").unwrap(), "[::1]");
        assert_eq!(local_probe_host("::1").unwrap(), "[::1]");
        assert_eq!(local_probe_host("127.0.0.1").unwrap(), "127.0.0.1");
        for unsafe_host in [
            "example.com",
            "8.8.8.8",
            "192.168.1.1",
            "[fe80::1]",
            "",
            "127.0.0.1@evil.test",
        ] {
            assert!(local_probe_host(unsafe_host).is_err(), "{unsafe_host}");
        }
    }
}
