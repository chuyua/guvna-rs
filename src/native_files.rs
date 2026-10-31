//! Verify opaque file IDs on the same upstream account before submitting work.
use super::AppState;
use crate::{config::Provider, keypool::Lease};
use serde_json::Value;
use std::{collections::BTreeSet, time::Duration};

pub(super) fn references(value: &Value) -> Result<Vec<String>, ()> {
    fn walk(value: &Value, ids: &mut BTreeSet<String>) -> Result<(), ()> {
        match value {
            Value::Object(map) => {
                if matches!(
                    map.get("type").and_then(Value::as_str),
                    Some("input_file" | "input_image")
                ) && let Some(id) = map.get("file_id").filter(|v| !v.is_null())
                {
                    let id = id
                        .as_str()
                        .filter(|id| !id.is_empty() && id.len() <= 512)
                        .ok_or(())?;
                    ids.insert(id.into());
                    if ids.len() > 16 {
                        return Err(());
                    }
                }
                for value in map.values() {
                    walk(value, ids)?;
                }
            }
            Value::Array(items) => {
                for value in items {
                    walk(value, ids)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut ids = BTreeSet::new();
    if let Some(input) = value.get("input") {
        walk(input, &mut ids)?;
    }
    Ok(ids.into_iter().collect())
}
pub(super) async fn verified(
    state: &AppState,
    provider: &Provider,
    lease: &Lease,
    ids: &[String],
    deadline: tokio::time::Instant,
) -> bool {
    for id in ids {
        let mut url =
            reqwest::Url::parse(&provider.endpoint("/files")).expect("validated provider");
        url.path_segments_mut().expect("HTTP URL").push(id);
        let operation = async {
            let mut response = state
                .client
                .get(url)
                .bearer_auth(&lease.secret)
                .send()
                .await
                .ok()?;
            if !response.status().is_success() {
                return None;
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.ok()? {
                if chunk.len() > 8192usize.saturating_sub(bytes.len()) {
                    return None;
                }
                bytes.extend_from_slice(&chunk);
            }
            let value: Value = serde_json::from_slice(&bytes).ok()?;
            (value["object"] == "file" && value["id"] == *id && value["status"] != "error")
                .then_some(())
        };
        let until = deadline.min(tokio::time::Instant::now() + Duration::from_secs(2));
        if !matches!(
            tokio::time::timeout_at(until, operation).await,
            Ok(Some(()))
        ) {
            return false;
        }
    }
    true
}
