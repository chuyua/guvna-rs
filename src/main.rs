use guvna::{chain_store, config::Config, gateway};
use std::{collections::HashMap, env, path::PathBuf};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("guvna: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
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
                .ok_or("chains requires list|validate|set|delete|recover")?,
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
                    "guvna serve [-config config.yaml] [-data DIR]\nguvna status [-config config.yaml]\nguvna logs [--tail 100] [--system]\n  --healthcheck probes /healthz\nguvna chains list|validate [-config FILE] [-data DIR]\nguvna chains set -file CHAIN.yaml [-config FILE] [-data DIR]\nguvna chains delete -name NAME [-config FILE] [-data DIR]\nguvna chains recover [-from BACKUP] [-config FILE] [-data DIR]\n  --version"
                );
                return Ok(());
            }
            other => return Err(format!("unknown or not-yet-migrated argument {other:?}")),
        }
    }

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
        let config = Config::load(&real_path)?;
        println!("{}", chain_store::execute(&config, &data_dir, command)?);
        return Ok(());
    }
    let config = Config::load(&real_path)?.with_runtime_chains(&data_dir)?;
    if healthcheck || status {
        let bind = env::var("GUVNA_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
        let probe_host = match bind.as_str() {
            "0.0.0.0" => "127.0.0.1",
            "::" | "[::]" => "[::1]",
            other if other.contains(':') && !other.starts_with('[') => {
                return Err("invalid GUVNA_BIND_ADDR for local healthcheck".into());
            }
            other => other,
        };
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
            println!("guvna running on 127.0.0.1:{}", config.port);
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
    let state = gateway::AppState::new(config, credentials, client_keys)?;
    let bind = env::var("GUVNA_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
    let listener = tokio::net::TcpListener::bind((bind.as_str(), port))
        .await
        .map_err(|e| format!("binding {bind}:{port}: {e}"))?;
    eprintln!(
        "guvna-rs starting; config={}; listening={bind}:{port}",
        real_path.display()
    );
    axum::serve(listener, gateway::app(state))
        .await
        .map_err(|e| format!("serving: {e}"))?;
    Ok(())
}
