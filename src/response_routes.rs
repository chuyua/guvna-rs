//! Bounded, credential- and caller-scoped native response ownership metadata.
//! Stores no prompts, outputs or secrets. Restart/TTL expiry fails closed.
use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};

const CAPACITY: usize = 1024;
const TTL: Duration = Duration::from_secs(3600);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Binding {
    pub(super) provider: usize,
    pub(super) credential: String,
    pub(super) model: String,
    pub(super) logical_model: String,
    pub(super) background: bool,
}
impl Binding {
    pub(super) fn same_origin(&self, other: &Self) -> bool {
        self.provider == other.provider
            && self.credential == other.credential
            && self.model == other.model
            && self.logical_model == other.logical_model
    }
}
#[derive(Clone)]
pub(super) struct Context {
    pub(super) owner: usize,
    pub(super) pin: Option<Binding>,
}
#[derive(Clone)]
pub(super) struct NativeOwner {
    pub(super) owner: usize,
    pub(super) logical_model: String,
    pub(super) background: bool,
    pub(super) stored: bool,
}
struct Entry {
    binding: Binding,
    created: Instant,
}
#[derive(Default)]
pub(super) struct ResponseRoutes {
    entries: Mutex<BTreeMap<(usize, u8, String), Entry>>,
}
impl ResponseRoutes {
    pub(super) fn get(&self, owner: usize, conversation: bool, id: &str) -> Option<Binding> {
        self.get_kind(owner, u8::from(conversation), id)
    }
    pub(super) fn get_item(&self, owner: usize, id: &str) -> Option<Binding> {
        self.get_kind(owner, 2, id)
    }
    fn get_kind(&self, owner: usize, kind: u8, id: &str) -> Option<Binding> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, v| v.created.elapsed() < TTL);
        entries
            .get(&(owner, kind, id.to_owned()))
            .map(|e| e.binding.clone())
    }
    #[cfg(test)]
    pub(super) fn remember(
        &self,
        owner: usize,
        conversation: bool,
        id: &str,
        binding: Binding,
    ) -> Result<(), ()> {
        self.remember_many(owner, &[(u8::from(conversation), id)], binding)
    }
    pub(super) fn remember_response(
        &self,
        owner: usize,
        id: Option<&str>,
        conversation: Option<&str>,
        items: &[&str],
        binding: Binding,
    ) -> Result<(), ()> {
        let mut ids = Vec::new();
        if let Some(id) = id {
            ids.push((0, id));
        }
        if let Some(id) = conversation {
            ids.push((1, id));
        }
        ids.extend(items.iter().map(|id| (2, *id)));
        self.remember_many(owner, &ids, binding)
    }
    fn remember_many(&self, owner: usize, ids: &[(u8, &str)], binding: Binding) -> Result<(), ()> {
        if ids.len() > 130
            || ids.iter().any(|(_, id)| id.is_empty() || id.len() > 512)
            || binding.model.len() > 512
            || binding.logical_model.len() > 512
            || binding.credential.len() > 512
        {
            return Err(());
        }
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, v| v.created.elapsed() < TTL);
        // Validate every key in one transaction before publishing either mapping.
        if ids.iter().any(|(conversation, id)| {
            entries
                .get(&(owner, *conversation, (*id).to_owned()))
                .is_some_and(|old| !old.binding.same_origin(&binding))
        }) {
            return Err(());
        }
        for (conversation, id) in ids {
            let key = (owner, *conversation, (*id).to_owned());
            if entries.contains_key(&key) {
                continue;
            }
            if entries.len() >= CAPACITY
                && let Some(old) = entries
                    .iter()
                    .min_by_key(|(_, v)| v.created)
                    .map(|(k, _)| k.clone())
            {
                entries.remove(&old);
            }
            entries.insert(
                key,
                Entry {
                    binding: binding.clone(),
                    created: Instant::now(),
                },
            );
        }
        Ok(())
    }
    pub(super) fn remove(&self, owner: usize, id: &str) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(owner, 0, id.to_owned()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn binding(provider: usize) -> Binding {
        Binding {
            provider,
            credential: "KEY".into(),
            model: "m".into(),
            logical_model: "demo".into(),
            background: false,
        }
    }
    #[test]
    fn bounded_scoped_collision_and_expiry() {
        let routes = ResponseRoutes::default();
        routes.remember(0, false, "same", binding(0)).unwrap();
        assert!(routes.get(1, false, "same").is_none());
        assert!(routes.get(0, true, "same").is_none());
        assert!(routes.remember(0, false, "same", binding(1)).is_err());
        assert!(
            routes
                .remember(0, false, &"x".repeat(513), binding(0))
                .is_err()
        );
        for i in 0..CAPACITY + 1 {
            routes
                .remember(0, false, &format!("id{i}"), binding(0))
                .unwrap();
        }
        assert_eq!(routes.entries.lock().unwrap().len(), CAPACITY);
        for entry in routes.entries.lock().unwrap().values_mut() {
            entry.created = Instant::now() - TTL;
        }
        assert!(routes.get(0, false, "id1024").is_none());
        assert!(routes.entries.lock().unwrap().is_empty());
    }
}
