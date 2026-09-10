//! Who is calling, and what they may do.
//!
//! Keys live in the database so they can be added and revoked without a
//! restart, and are cached in the process for a short while so a busy caller
//! does not cost a database read per request. The cache is deliberately short:
//! revoking a key should take effect in seconds, not when a pod happens to
//! restart.

use crate::config::Api as ApiCfg;
use crate::store::{ApiKey, Store};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// What went wrong with a caller's credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    /// No key was supplied at all.
    Missing,
    /// The key is not one we know, or has been disabled.
    Unknown,
    /// A known key that may not do this particular thing.
    NotPermitted,
}

impl AuthError {
    pub fn message(self) -> &'static str {
        match self {
            AuthError::Missing => "No API key was supplied",
            AuthError::Unknown => "Invalid API key",
            AuthError::NotPermitted => "This API key may not use this endpoint",
        }
    }
}

/// What a caller wants to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Locate,
    Submit,
    Admin,
}

/// A short-lived in-process cache of API keys.
pub struct KeyCache {
    entries: Mutex<HashMap<String, (Option<ApiKey>, Instant)>>,
    ttl: Duration,
    require_keys: bool,
}

impl KeyCache {
    pub fn new(cfg: &ApiCfg) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl: Duration::from_secs(cfg.key_cache_secs),
            require_keys: cfg.require_keys,
        }
    }

    /// Check a caller's key and permission.
    ///
    /// When keys are not required, any caller is allowed everything. That is
    /// for local development only; `api.require_keys` defaults to on precisely
    /// so a service reachable by anyone cannot be left open by omission.
    pub async fn check<S: Store + ?Sized>(
        &self,
        store: &S,
        key: Option<&str>,
        permission: Permission,
    ) -> Result<Option<ApiKey>, AuthError> {
        if !self.require_keys {
            return Ok(None);
        }

        let key = key.filter(|k| !k.is_empty()).ok_or(AuthError::Missing)?;
        let found = self.lookup(store, key).await;

        let api_key = found.ok_or(AuthError::Unknown)?;
        if !api_key.enabled {
            return Err(AuthError::Unknown);
        }

        let allowed = match permission {
            Permission::Locate => api_key.allow_locate,
            Permission::Submit => api_key.allow_submit,
            Permission::Admin => api_key.allow_admin,
        };
        if !allowed {
            return Err(AuthError::NotPermitted);
        }
        Ok(Some(api_key))
    }

    async fn lookup<S: Store + ?Sized>(&self, store: &S, key: &str) -> Option<ApiKey> {
        if let Some(cached) = self.cached(key) {
            return cached;
        }
        // A store failure caches nothing, so the next request tries again
        // rather than being locked out for the whole cache lifetime.
        let found = store.get_api_key(key).await.ok()?;
        self.remember(key, found.clone());
        found
    }

    fn cached(&self, key: &str) -> Option<Option<ApiKey>> {
        let mut entries = self.entries.lock().expect("key cache poisoned");
        match entries.get(key) {
            Some((value, until)) if *until > Instant::now() => Some(value.clone()),
            Some(_) => {
                entries.remove(key);
                None
            }
            None => None,
        }
    }

    fn remember(&self, key: &str, value: Option<ApiKey>) {
        self.entries
            .lock()
            .expect("key cache poisoned")
            .insert(key.to_string(), (value, Instant::now() + self.ttl));
    }

    /// Forget a key at once, for tests and for an administrative revoke.
    pub fn forget(&self, key: &str) {
        self.entries.lock().expect("key cache poisoned").remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    fn cfg() -> ApiCfg {
        ApiCfg::default()
    }

    async fn store_with(key: ApiKey) -> MemoryStore {
        let store = MemoryStore::new();
        store.put_api_key(&key).await.unwrap();
        store
    }

    #[tokio::test]
    async fn a_valid_key_is_accepted() {
        let store = store_with(ApiKey::full("good")).await;
        let cache = KeyCache::new(&cfg());
        assert!(cache.check(&store, Some("good"), Permission::Locate).await.is_ok());
    }

    #[tokio::test]
    async fn a_missing_key_is_refused_distinctly_from_a_wrong_one() {
        let store = store_with(ApiKey::full("good")).await;
        let cache = KeyCache::new(&cfg());
        assert_eq!(
            cache.check(&store, None, Permission::Locate).await,
            Err(AuthError::Missing)
        );
        assert_eq!(
            cache.check(&store, Some(""), Permission::Locate).await,
            Err(AuthError::Missing)
        );
        assert_eq!(
            cache.check(&store, Some("nope"), Permission::Locate).await,
            Err(AuthError::Unknown)
        );
    }

    #[tokio::test]
    async fn a_disabled_key_is_refused() {
        let key = ApiKey { enabled: false, ..ApiKey::full("retired") };
        let store = store_with(key).await;
        let cache = KeyCache::new(&cfg());
        assert_eq!(
            cache.check(&store, Some("retired"), Permission::Locate).await,
            Err(AuthError::Unknown)
        );
    }

    #[tokio::test]
    async fn permissions_are_enforced_one_by_one() {
        let key = ApiKey {
            allow_locate: true,
            allow_submit: false,
            allow_admin: false,
            ..ApiKey::full("reader")
        };
        let store = store_with(key).await;
        let cache = KeyCache::new(&cfg());

        assert!(cache.check(&store, Some("reader"), Permission::Locate).await.is_ok());
        assert_eq!(
            cache.check(&store, Some("reader"), Permission::Submit).await,
            Err(AuthError::NotPermitted)
        );
        assert_eq!(
            cache.check(&store, Some("reader"), Permission::Admin).await,
            Err(AuthError::NotPermitted)
        );
    }

    #[tokio::test]
    async fn a_repeated_caller_does_not_cost_a_lookup_each_time() {
        let store = store_with(ApiKey::full("busy")).await;
        let cache = KeyCache::new(&cfg());

        cache.check(&store, Some("busy"), Permission::Locate).await.unwrap();
        let after_first = store.read_count();
        for _ in 0..20 {
            cache.check(&store, Some("busy"), Permission::Locate).await.unwrap();
        }
        assert_eq!(store.read_count(), after_first, "the cache absorbed them");
    }

    #[tokio::test]
    async fn an_unknown_key_is_also_cached_so_a_flood_costs_one_lookup() {
        let store = store_with(ApiKey::full("good")).await;
        let cache = KeyCache::new(&cfg());

        for _ in 0..20 {
            assert!(cache.check(&store, Some("bogus"), Permission::Locate).await.is_err());
        }
        // Without caching the negative, an attacker guessing keys would drive a
        // database read per attempt.
        assert!(store.read_count() <= 2);
    }

    #[tokio::test]
    async fn a_revoked_key_stops_working_once_forgotten() {
        let store = store_with(ApiKey::full("temp")).await;
        let cache = KeyCache::new(&cfg());
        cache.check(&store, Some("temp"), Permission::Locate).await.unwrap();

        store.put_api_key(&ApiKey { enabled: false, ..ApiKey::full("temp") }).await.unwrap();
        cache.forget("temp");

        assert_eq!(
            cache.check(&store, Some("temp"), Permission::Locate).await,
            Err(AuthError::Unknown)
        );
    }

    #[tokio::test]
    async fn a_store_failure_does_not_lock_a_caller_out_for_the_whole_cache_lifetime() {
        let store = store_with(ApiKey::full("good")).await;
        let cache = KeyCache::new(&cfg());

        store.set_failing(true);
        assert!(cache.check(&store, Some("good"), Permission::Locate).await.is_err());

        store.set_failing(false);
        assert!(
            cache.check(&store, Some("good"), Permission::Locate).await.is_ok(),
            "the failure must not have been cached"
        );
    }

    #[tokio::test]
    async fn turning_keys_off_allows_everything() {
        // Development convenience. It defaults to off precisely so a service
        // reachable by anyone cannot be left open by omission.
        let store = MemoryStore::new();
        let cfg = ApiCfg { require_keys: false, ..ApiCfg::default() };
        let cache = KeyCache::new(&cfg);
        assert!(cache.check(&store, None, Permission::Admin).await.is_ok());
    }

    #[tokio::test]
    async fn requiring_keys_is_the_default() {
        assert!(ApiCfg::default().require_keys);
    }
}
