//! Permission-restricted, read-only local runtime status over a Unix domain socket.
//! Same-UID Unix socket supports limited runtime chain edits. No TCP admin.
use guvna::{chain_store, config::Config, gateway::AppState};
use serde_json::{Value, json};
use std::{
    fs, io,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::Semaphore,
    task::JoinHandle,
    time::timeout,
};

const SOCKET_NAME: &str = "control.sock";
const MAX_CONTROL_LINE: usize = 4096;
const MAX_CONTROL_RESPONSE_LINE: usize = 32768;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_CLIENTS: usize = 4;

struct ControlSocket {
    listener: UnixListener,
    path: PathBuf,
    directory: PathBuf,
    state: Arc<AppState>,
    base_config: Arc<Config>,
}

impl ControlSocket {
    async fn bind(
        data_dir: &Path,
        state: Arc<AppState>,
        base_config: Arc<Config>,
    ) -> Result<Self, String> {
        let path = socket_path(data_dir)?;
        let newly_created = !data_dir.exists();
        fs::create_dir_all(data_dir)
            .map_err(|error| format!("create local control directory: {error}"))?;
        let directory_metadata = fs::symlink_metadata(data_dir)
            .map_err(|error| format!("inspect local control directory: {error}"))?;
        if !directory_metadata.file_type().is_dir() {
            return Err("GUVNA_DATA_DIR must be a real directory, not a symlink".into());
        }
        if newly_created {
            fs::set_permissions(data_dir, fs::Permissions::from_mode(0o700))
                .map_err(|error| format!("secure local control directory: {error}"))?;
        }
        ensure_socket_available(&path).await?;
        let listener = UnixListener::bind(&path)
            .map_err(|error| format!("bind local control socket {}: {error}", path.display()))?;
        if let Err(error) = fs::set_permissions(&path, fs::Permissions::from_mode(0o600)) {
            let _ = fs::remove_file(&path);
            return Err(format!("secure local control socket: {error}"));
        }
        Ok(Self {
            listener,
            path,
            directory: data_dir.to_path_buf(),
            state,
            base_config,
        })
    }

    async fn run(self) {
        let permits = Arc::new(Semaphore::new(MAX_CLIENTS));
        loop {
            let (stream, _) = match self.listener.accept().await {
                Ok(connection) => connection,
                Err(_) => break,
            };
            let permit = match permits.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let state = Arc::clone(&self.state);
            let directory = self.directory.clone();
            let base_config = Arc::clone(&self.base_config);
            tokio::spawn(async move {
                let _permit = permit;
                let _ = handle_client(stream, state, directory, base_config).await;
            });
        }
    }
}

impl Drop for ControlSocket {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path).is_ok_and(|metadata| metadata.file_type().is_socket()) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn socket_path(data_dir: &Path) -> Result<PathBuf, String> {
    let path = data_dir.join(SOCKET_NAME);
    // Linux sun_path is 108 bytes including its trailing NUL.
    if path.as_os_str().as_bytes().len() >= 108 {
        return Err(
            "GUVNA_DATA_DIR is too long for a Unix control socket; use a shorter path".into(),
        );
    }
    Ok(path)
}

async fn ensure_socket_available(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("inspect local control socket: {error}")),
        Ok(metadata) if !metadata.file_type().is_socket() => {
            Err("control socket path exists but is not a socket; refusing to replace it".into())
        }
        Ok(_) => match timeout(CONTROL_TIMEOUT, UnixStream::connect(path)).await {
            Ok(Ok(_)) => Err("another guvna process already owns the local control socket".into()),
            Ok(Err(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                ) =>
            {
                match fs::remove_file(path) {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(format!("remove stale local control socket: {error}")),
                }
            }
            Ok(Err(error)) => Err(format!("check local control socket: {error}")),
            Err(_) => Err("timed out checking local control socket".into()),
        },
    }
}

pub async fn spawn(
    data_dir: &Path,
    state: Arc<AppState>,
    base_config: Arc<Config>,
) -> Result<JoinHandle<()>, String> {
    let server = ControlSocket::bind(data_dir, state, base_config).await?;
    Ok(tokio::spawn(server.run()))
}

async fn read_line_bounded<R: AsyncRead + Unpin>(
    reader: &mut R,
    limit: usize,
) -> Result<Vec<u8>, &'static str> {
    let mut bytes = Vec::with_capacity(128);
    loop {
        let mut byte = [0_u8; 1];
        let read = reader.read(&mut byte).await.map_err(|_| "read_failed")?;
        if read == 0 {
            return if bytes.is_empty() {
                Err("empty_request")
            } else {
                Ok(bytes)
            };
        }
        if byte[0] == b'\n' {
            return Ok(bytes);
        }
        if bytes.len() >= limit {
            return Err("request_too_large");
        }
        bytes.push(byte[0]);
    }
}

async fn handle_client(
    mut stream: UnixStream,
    state: Arc<AppState>,
    directory: PathBuf,
    base_config: Arc<Config>,
) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    if stream
        .peer_cred()
        .map_err(|_| "control peer credentials unavailable")?
        .uid()
        != unsafe { libc::geteuid() }
    {
        return Err("control peer uid mismatch".into());
    }
    let bytes = timeout(
        CONTROL_TIMEOUT,
        read_line_bounded(&mut stream, MAX_CONTROL_LINE),
    )
    .await
    .map_err(|_| "control request timed out")?
    .map_err(str::to_string)?;
    let request: Value = serde_json::from_slice(&bytes).map_err(|_| "invalid control JSON")?;
    let response = match request.get("command").and_then(Value::as_str) {
        Some("status") => state.runtime_status(),
        Some("chains_list") => {
            let snapshot = state.route_snapshot();
            json!({"chains":snapshot.chains.iter().map(|c| &c.name).collect::<Vec<_>>()})
        }
        Some("chains_set" | "chains_delete") => {
            let action = request.get("command").and_then(Value::as_str).unwrap_or("");
            let value = request
                .get("argument")
                .and_then(Value::as_str)
                .unwrap_or("");
            if value.is_empty() || value.len() > 2048 {
                json!({"error":"invalid_chain_argument"})
            } else {
                let cmd = if action == "chains_set" {
                    chain_store::Command::Set(PathBuf::from(value))
                } else {
                    chain_store::Command::Delete(value.to_owned())
                };
                let base = Arc::clone(&base_config);
                let local_dir = directory.clone();
                let result = timeout(
                    Duration::from_secs(3),
                    tokio::task::spawn_blocking(move || {
                        let output = chain_store::execute(&base, &local_dir, cmd)?;
                        let routes = base.as_ref().clone().with_runtime_chains(&local_dir)?;
                        Ok::<_, String>((output, routes))
                    }),
                )
                .await;
                match result {
                    Ok(Ok(Ok((output, routes)))) => match state.replace_route_snapshot(routes) {
                        Ok(()) => json!({"ok":true,"online":true,"result":output}),
                        Err(_) => json!({"error":"route_reload_failed"}),
                    },
                    _ => json!({"error":"chain_update_failed"}),
                }
            }
        }
        Some("scoring") => {
            let hours = request.get("hours").and_then(Value::as_u64).unwrap_or(24);
            if ![1, 6, 24, 168].contains(&hours) {
                json!({"error":"invalid_scoring_window"})
            } else {
                let work = tokio::task::spawn_blocking(move || {
                    guvna::scoring::read_scoring(&directory, hours as u32)
                });
                match timeout(Duration::from_millis(800), work).await {
                    Ok(Ok(Ok(report))) => report,
                    _ => json!({"error":"scoring_unavailable"}),
                }
            }
        }
        _ => json!({"error":"unsupported_command"}),
    };
    let mut output = serde_json::to_vec(&response).map_err(|_| "encode control response")?;
    if output.len() >= MAX_CONTROL_RESPONSE_LINE {
        output = serde_json::to_vec(&json!({"error":"control_response_too_large"}))
            .map_err(|_| "encode bounded control response")?;
    }
    output.push(b'\n');
    timeout(CONTROL_TIMEOUT, stream.write_all(&output))
        .await
        .map_err(|_| "control response timed out")?
        .map_err(|_| "write control response")?;
    Ok(())
}

/// Returns None only when no socket is installed (legacy HTTP health probe fallback).
pub async fn query(data_dir: &Path) -> Result<Option<String>, String> {
    query_request(data_dir, json!({"command":"status"}), CONTROL_TIMEOUT).await
}
pub async fn query_scoring(data_dir: &Path, hours: u32) -> Result<Option<String>, String> {
    query_request(
        data_dir,
        json!({"command":"scoring","hours":hours}),
        CONTROL_TIMEOUT,
    )
    .await
}
pub async fn query_chain_mutation(
    data_dir: &Path,
    action: &str,
    argument: &str,
) -> Result<Option<String>, String> {
    if !["chains_set", "chains_delete"].contains(&action) {
        return Err("unsupported local chain operation".into());
    }
    query_request(
        data_dir,
        json!({"command":action,"argument":argument}),
        Duration::from_secs(5),
    )
    .await
}
async fn query_request(
    data_dir: &Path,
    request: Value,
    reply_timeout: Duration,
) -> Result<Option<String>, String> {
    let path = socket_path(data_dir)?;
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("inspect local control socket: {error}")),
        Ok(metadata) if !metadata.file_type().is_socket() => {
            return Err("control socket path exists but is not a socket".into());
        }
        Ok(_) => {}
    }
    let mut stream = match timeout(CONTROL_TIMEOUT, UnixStream::connect(&path)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) if error.kind() == io::ErrorKind::ConnectionRefused => {
            // A stale socket is equivalent to absence for status probing.
            return Ok(None);
        }
        Ok(Err(error)) => return Err(format!("connect local control socket: {error}")),
        Err(_) => return Err("timed out connecting to local control socket".into()),
    };
    let mut request =
        serde_json::to_vec(&request).map_err(|_| "cannot serialize control request")?;
    request.push(b'\n');
    timeout(CONTROL_TIMEOUT, stream.write_all(&request))
        .await
        .map_err(|_| "local status request timed out")?
        .map_err(|_| "write local status request")?;
    let bytes = timeout(
        reply_timeout,
        read_line_bounded(&mut stream, MAX_CONTROL_RESPONSE_LINE),
    )
    .await
    .map_err(|_| "local status response timed out")?
    .map_err(str::to_string)?;
    let response: Value =
        serde_json::from_slice(&bytes).map_err(|_| "invalid local status response")?;
    if let Some(error) = response.get("error").and_then(Value::as_str) {
        return Err(format!("local status query failed: {error}"));
    }
    serde_json::to_string_pretty(&response)
        .map(Some)
        .map_err(|_| "format local status response".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn local_status_round_trip_is_private_and_reports_runtime_counters() {
        use guvna::config::Config;
        use std::collections::HashMap;

        let dir = tempfile::tempdir().unwrap();
        let original_directory_mode =
            fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777;
        let config: Config = serde_yaml::from_str(
            "providers:\n  - name: private-provider\n    type: openai\n    base_url: http://127.0.0.1:1\n    key_env: UPSTREAM_SECRET_ENV\n    models: [test-model]\n"
        ).unwrap();
        let state = Arc::new(
            AppState::new(
                config.clone(),
                HashMap::from([(
                    "UPSTREAM_SECRET_ENV".to_string(),
                    "provider-secret".to_string(),
                )]),
                vec!["client-secret".to_string()],
            )
            .unwrap(),
        );
        let server = spawn(dir.path(), state, Arc::new(config)).await.unwrap();
        let socket = dir.path().join(SOCKET_NAME);
        let mode = fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let directory_mode = fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            directory_mode, original_directory_mode,
            "existing user-supplied directory permissions must be preserved"
        );

        let response = query(dir.path()).await.unwrap().unwrap();
        let value: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(value["status"], "running");
        assert_eq!(value["providers_configured"], 1);
        assert_eq!(value["requests_dispatched"], 0);
        assert!(!response.contains("provider-secret"));
        assert!(!response.contains("client-secret"));
        assert!(!response.contains("private-provider"));
        assert!(!response.contains("UPSTREAM_SECRET_ENV"));

        let scoring = query_scoring(dir.path(), 1).await.unwrap().unwrap();
        let scoring_data: Value = serde_json::from_str(&scoring).unwrap();
        assert_eq!(scoring_data["enabled"], false);
        assert_eq!(scoring_data["window_hours"], 1);
        assert!(query_scoring(dir.path(), 2).await.is_err());

        server.abort();
        let _ = server.await;
        assert!(!socket.exists());
    }
    #[tokio::test]
    async fn online_chain_mutations_update_live_routes_without_touching_static_routes() {
        use guvna::{config::Config, routing};
        use std::collections::HashMap;

        let dir = tempfile::tempdir().unwrap();
        let config: Config = serde_yaml::from_str(
            "providers:\n  - name: p\n    type: openai\n    base_url: http://127.0.0.1:1\n    key_env: K\nchains:\n  - name: immutable\n    steps:\n      - provider: p\n        model: static\n"
        ).unwrap();
        let state = Arc::new(
            AppState::new(
                config.clone(),
                HashMap::from([("K".to_owned(), "placeholder".to_owned())]),
                vec!["local-client".to_owned()],
            )
            .unwrap(),
        );
        let server = spawn(dir.path(), Arc::clone(&state), Arc::new(config))
            .await
            .unwrap();
        let definition = dir.path().join("new-chain.yaml");
        fs::write(
            &definition,
            "name: dynamic\nsteps:\n  - provider: p\n    model: fresh\n",
        )
        .unwrap();
        let before = state.route_snapshot();
        assert!(routing::resolve(&before, "dynamic").is_err());
        let result = query_chain_mutation(dir.path(), "chains_set", definition.to_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(result.contains("online"));
        let after = state.route_snapshot();
        assert_eq!(
            routing::resolve(&after, "dynamic").unwrap()[0].model,
            "fresh"
        );
        assert_eq!(
            routing::resolve(&before, "immutable").unwrap()[0].model,
            "static"
        );
        assert!(routing::resolve(&before, "dynamic").is_err());
        assert!(after.chains.iter().any(|chain| chain.name == "dynamic"));
        let delete = query_chain_mutation(dir.path(), "chains_delete", "dynamic")
            .await
            .unwrap()
            .unwrap();
        assert!(delete.contains("online"));
        let latest = state.route_snapshot();
        assert!(routing::resolve(&latest, "dynamic").is_err());
        assert!(routing::resolve(&latest, "immutable").is_ok());
        let forbidden = query_chain_mutation(dir.path(), "chains_delete", "immutable")
            .await
            .unwrap_err();
        assert!(forbidden.contains("chain_update_failed"));
        assert!(routing::resolve(&state.route_snapshot(), "immutable").is_ok());
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn oversized_control_lines_are_rejected() {
        let (mut writer, mut reader) = tokio::io::duplex(MAX_CONTROL_LINE + 32);
        writer
            .write_all(&vec![b'x'; MAX_CONTROL_LINE + 1])
            .await
            .unwrap();
        assert_eq!(
            read_line_bounded(&mut reader, MAX_CONTROL_LINE).await,
            Err("request_too_large")
        );
    }

    #[tokio::test]
    async fn non_socket_paths_are_never_removed_or_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(SOCKET_NAME);
        fs::write(&path, b"keep this file").unwrap();
        assert!(ensure_socket_available(&path).await.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"keep this file");
    }

    #[tokio::test]
    async fn stale_socket_is_removed_but_active_socket_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let stale = dir.path().join(SOCKET_NAME);
        drop(UnixListener::bind(&stale).unwrap());
        ensure_socket_available(&stale).await.unwrap();
        assert!(!stale.exists());

        let active = dir.path().join("active.sock");
        let listener = UnixListener::bind(&active).unwrap();
        assert!(ensure_socket_available(&active).await.is_err());
        assert!(active.exists());
        drop(listener);
    }

    #[test]
    fn rejects_overlong_socket_paths_before_touching_the_directory() {
        let long_dir = PathBuf::from(format!(
            "{}{}",
            std::env::temp_dir().display(),
            "x".repeat(120)
        ));
        assert!(socket_path(&long_dir).is_err());
    }
}
