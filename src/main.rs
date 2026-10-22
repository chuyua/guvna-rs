use guvna::{config::Config, gateway};
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
    let mut args = env::args().skip(1).peekable();
    if args.peek().is_some_and(|s| s == "serve") {
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
            "-healthcheck" | "--healthcheck" => healthcheck = true,
            "--version" | "-version" => {
                println!("guvna {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "--help" | "-h" | "help" => {
                println!(
                    "guvna serve [-config config.yaml] [-data DIR]\n  --healthcheck probes /healthz\n  --version"
                );
                return Ok(());
            }
            other => return Err(format!("unknown or not-yet-migrated argument {other:?}")),
        }
    }

    let real_path = std::fs::canonicalize(&config_path)
        .map_err(|e| format!("config path {}: {e}", config_path.display()))?;
    let config = Config::load(&real_path)?.with_runtime_chains(&data_dir)?;
    if healthcheck {
        let url = format!("http://127.0.0.1:{}/healthz", config.port);
        let response = reqwest::Client::new()
            .get(url)
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await
            .map_err(|e| format!("healthcheck: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("healthcheck HTTP {}", response.status()));
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
