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
    /// The key could not be checked, because storage is unreachable.
    ///
    /// Distinct from [`AuthError::Unknown`] on purpose. Telling a caller with a
    /// perfectly good key that it is invalid sends whoever investigates in
    /// exactly the wrong direction.
    Unavailable,
}

impl AuthError {
    pub fn message(self) -> &'static str {
        match self {
            AuthError::Missing => "No API key was supplied",
            AuthError::Unknown => "Invalid API key",
            AuthError::NotPermitted => "This API key may not use this endpoint",
            AuthError::Unavailable => "Could not verify the API key: storage is unavailable",
        }
    }

    /// Whether this is the caller's fault or ours.
    /// A stable metric label, so a dashboard can show why callers are being
    /// turned away without anyone reading the logs.
    pub fn as_str(self) -> &'static str {
        match self {
            AuthError::Missing => "missing",
            AuthError::Unknown => "unknown",
            AuthError::NotPermitted => "not_permitted",
            AuthError::Unavailable => "unavailable",
        }
    }

    pub fn is_our_fault(self) -> bool {
        matches!(self, AuthError::Unavailable)
    }
}

/// What a caller wants to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Locate,
    Submit,
    Admin,
}

/// Cache size past which expired entries are swept on each insert.
const SWEEP_ABOVE: usize = 10_000;

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
    /// for local development only; `api.requireKeys` defaults to on precisely
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
        let found = self.lookup(store, key).await?;

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

    async fn lookup<S: Store + ?Sized>(
        &self,
        store: &S,
        key: &str,
    ) -> Result<Option<ApiKey>, AuthError> {
        // A key checked recently keeps working through an outage. That is the
        // point of the cache here: a storage problem should not lock out the
        // callers who were already using the service.
        if let Some(cached) = self.cached(key) {
            return Ok(cached);
        }
        // A store failure caches nothing, so the next request tries again
        // rather than being locked out for the whole cache lifetime.
        let found = store.get_api_key(key).await.map_err(|_| AuthError::Unavailable)?;
        self.remember(key, found.clone());
        Ok(found)
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
        let now = Instant::now();
        let mut entries = self.entries.lock().expect("key cache poisoned");
        // An entry is otherwise dropped only when its own key is asked for
        // again, so a flood of made-up keys, each asked once, would grow the
        // cache without end. Past a size, expired entries are swept, which
        // bounds the cache by what arrives within one lifetime.
        if entries.len() >= SWEEP_ABOVE {
            entries.retain(|_, (_, until)| *until > now);
        }
        entries.insert(key.to_string(), (value, now + self.ttl));
    }

    /// How many keys are cached, expired or not.
    pub fn len(&self) -> usize {
        self.entries.lock().expect("key cache poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    #[test]
    fn every_auth_failure_has_its_own_label() {
        let all = [
            AuthError::Missing,
            AuthError::Unknown,
            AuthError::NotPermitted,
            AuthError::Unavailable,
        ];
        let mut seen = std::collections::HashSet::new();
        for error in all {
            assert!(seen.insert(error.as_str()), "{} is used twice", error.as_str());
        }
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
        assert_eq!(cache.check(&store, None, Permission::Locate).await, Err(AuthError::Missing));
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
    async fn a_storage_outage_is_reported_as_ours_not_as_a_bad_key() {
        // Telling a caller with a perfectly good key that it is invalid sends
        // whoever investigates in exactly the wrong direction.
        let store = store_with(ApiKey::full("good")).await;
        let cache = KeyCache::new(&cfg());

        store.set_failing(true);
        let error = cache.check(&store, Some("good"), Permission::Locate).await.unwrap_err();
        assert_eq!(error, AuthError::Unavailable);
        assert!(error.is_our_fault());

        store.set_failing(false);
        assert!(
            cache.check(&store, Some("good"), Permission::Locate).await.is_ok(),
            "the failure must not have been cached"
        );
    }

    #[tokio::test]
    async fn a_caller_already_using_the_service_survives_an_outage() {
        let store = store_with(ApiKey::full("busy")).await;
        let cache = KeyCache::new(&cfg());

        cache.check(&store, Some("busy"), Permission::Locate).await.unwrap();
        store.set_failing(true);

        assert!(
            cache.check(&store, Some("busy"), Permission::Locate).await.is_ok(),
            "a key checked moments ago should keep working"
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
    async fn a_flood_of_made_up_keys_does_not_grow_the_cache_for_ever() {
        let store = MemoryStore::new();
        let cache = KeyCache::new(&ApiCfg { key_cache_secs: 0, ..cfg() });
        for i in 0..SWEEP_ABOVE + 500 {
            let _ = cache.check(&store, Some(&format!("bogus-{i}")), Permission::Locate).await;
        }
        // With a lifetime of zero every entry has expired by the next insert,
        // so each sweep leaves one.
        assert!(cache.len() <= SWEEP_ABOVE, "{} entries", cache.len());
    }

    #[tokio::test]
    async fn live_entries_survive_a_sweep() {
        let store = store_with(ApiKey::full("busy")).await;
        let cache = KeyCache::new(&cfg());
        for i in 0..SWEEP_ABOVE + 10 {
            let _ = cache.check(&store, Some(&format!("bogus-{i}")), Permission::Locate).await;
        }
        assert_eq!(cache.len(), SWEEP_ABOVE + 10, "nothing had expired, so nothing went");
    }

    #[tokio::test]
    async fn requiring_keys_is_the_default() {
        assert!(ApiCfg::default().require_keys);
    }
}
