//! Station identity: what makes one access point or cell tower distinct.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Radio technology of a cell. Values match Ichnaea's `Radio` enum, and they
/// are stored, so they must not be renumbered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(i8)]
pub enum Radio {
    Gsm = 0,
    Cdma = 1,
    Wcdma = 2,
    Lte = 3,
}

impl Radio {
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Radio::Gsm),
            1 => Some(Radio::Cdma),
            2 => Some(Radio::Wcdma),
            3 => Some(Radio::Lte),
            _ => None,
        }
    }

    pub fn as_i8(self) -> i8 {
        self as i8
    }

    /// Accepts the spellings clients actually send. `umts` and `lte_nr` show up
    /// in the wild alongside the canonical names.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "gsm" => Some(Radio::Gsm),
            "cdma" => Some(Radio::Cdma),
            "wcdma" | "umts" | "hspa" => Some(Radio::Wcdma),
            "lte" | "lte_nr" | "nr" => Some(Radio::Lte),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Radio::Gsm => "gsm",
            Radio::Cdma => "cdma",
            Radio::Wcdma => "wcdma",
            Radio::Lte => "lte",
        }
    }
}

impl fmt::Display for Radio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A cell tower's identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CellKey {
    pub radio: Radio,
    pub mcc: i32,
    pub mnc: i32,
    pub lac: i64,
    pub cid: i64,
}

impl fmt::Display for CellKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}:{}:{}", self.radio, self.mcc, self.mnc, self.lac, self.cid)
    }
}

/// A WiFi access point's identity: 48 bits of address in a u64.
///
/// Clients send twelve hex characters, which may be a real BSSID or the first
/// twelve characters of a hash of one. Either way it is treated as an opaque
/// identifier and stored as an integer, which is denser than bytes in both the
/// row and the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WifiKey(pub u64);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyError {
    #[error("expected 12 hexadecimal characters, got {0}")]
    BadLength(usize),
    #[error("not hexadecimal: {0:?}")]
    NotHex(char),
}

impl WifiKey {
    /// Parse twelve hex characters. Case is preserved in meaning only by being
    /// ignored: the same address written either way is the same station.
    ///
    /// Separators are not accepted. Clients send a bare twelve-character string
    /// and accepting `aa:bb:...` too would let one access point acquire two
    /// identities depending on which client saw it.
    pub fn parse(s: &str) -> Result<Self, KeyError> {
        if s.len() != 12 {
            return Err(KeyError::BadLength(s.len()));
        }
        let mut value: u64 = 0;
        for ch in s.chars() {
            let digit = ch.to_digit(16).ok_or(KeyError::NotHex(ch))?;
            value = (value << 4) | u64::from(digit);
        }
        Ok(WifiKey(value))
    }

    /// The twelve-character lower-case form, as clients send it.
    pub fn to_hex(self) -> String {
        format!("{:012x}", self.0)
    }
}

impl fmt::Display for WifiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Either kind of station, used wherever the two paths share code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StationKey {
    Wifi(WifiKey),
    Cell(CellKey),
}

/// Which table a station lives in. Stored in the spatial index, so the numbers
/// must not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(i8)]
pub enum StationKind {
    Wifi = 0,
    Cell = 1,
}

impl StationKey {
    pub fn kind(&self) -> StationKind {
        match self {
            StationKey::Wifi(_) => StationKind::Wifi,
            StationKey::Cell(_) => StationKind::Cell,
        }
    }

    /// Which queue shard owns this station.
    ///
    /// Every observation of one station must land in the same shard, or two
    /// workers could fold the same station at once and the whole lock-free
    /// design falls apart. So this hash has to be stable across processes,
    /// restarts and compiler versions, which rules out the standard library's
    /// default hasher.
    pub fn shard(&self, shards: u32) -> u32 {
        debug_assert!(shards > 0, "shard count is validated at startup");
        (fnv1a64(&self.shard_bytes()) % u64::from(shards.max(1))) as u32
    }

    fn shard_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(40);
        match self {
            StationKey::Wifi(w) => {
                out.push(0u8);
                out.extend_from_slice(&w.0.to_be_bytes());
            }
            StationKey::Cell(c) => {
                out.push(1u8);
                out.push(c.radio.as_i8() as u8);
                out.extend_from_slice(&c.mcc.to_be_bytes());
                out.extend_from_slice(&c.mnc.to_be_bytes());
                out.extend_from_slice(&c.lac.to_be_bytes());
                out.extend_from_slice(&c.cid.to_be_bytes());
            }
        }
        out
    }

    /// Stable text form, used for cache keys and queue messages.
    pub fn to_token(&self) -> String {
        match self {
            StationKey::Wifi(w) => format!("w:{}", w.to_hex()),
            StationKey::Cell(c) => {
                format!("c:{}:{}:{}:{}:{}", c.radio.as_i8(), c.mcc, c.mnc, c.lac, c.cid)
            }
        }
    }

    /// Inverse of [`StationKey::to_token`].
    pub fn from_token(s: &str) -> Option<Self> {
        let (tag, rest) = s.split_once(':')?;
        match tag {
            "w" => WifiKey::parse(rest).ok().map(StationKey::Wifi),
            "c" => {
                let mut it = rest.split(':');
                let radio = Radio::from_i8(it.next()?.parse().ok()?)?;
                let mcc = it.next()?.parse().ok()?;
                let mnc = it.next()?.parse().ok()?;
                let lac = it.next()?.parse().ok()?;
                let cid = it.next()?.parse().ok()?;
                if it.next().is_some() {
                    return None;
                }
                Some(StationKey::Cell(CellKey { radio, mcc, mnc, lac, cid }))
            }
            _ => None,
        }
    }
}

impl fmt::Display for StationKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_token())
    }
}

/// FNV-1a, 64-bit. Chosen for being tiny and specified, not for speed: the
/// property that matters is that it gives the same answer everywhere, forever.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell() -> CellKey {
        CellKey { radio: Radio::Lte, mcc: 432, mnc: 11, lac: 1234, cid: 56789 }
    }

    #[test]
    fn wifi_keys_parse_from_twelve_hex_characters() {
        assert_eq!(WifiKey::parse("aabbccddeeff").unwrap().0, 0xaabb_ccdd_eeff);
        assert_eq!(WifiKey::parse("000000000000").unwrap().0, 0);
        assert_eq!(WifiKey::parse("ffffffffffff").unwrap().0, 0xffff_ffff_ffff);
    }

    #[test]
    fn wifi_key_parsing_ignores_case() {
        assert_eq!(WifiKey::parse("AABBCCDDEEFF"), WifiKey::parse("aabbccddeeff"));
    }

    #[test]
    fn wifi_keys_round_trip_through_hex() {
        for raw in ["aabbccddeeff", "000000000001", "0123456789ab"] {
            assert_eq!(WifiKey::parse(raw).unwrap().to_hex(), raw);
        }
    }

    #[test]
    fn wrong_length_and_non_hex_are_rejected() {
        assert_eq!(WifiKey::parse("aabbccddeef"), Err(KeyError::BadLength(11)));
        assert_eq!(WifiKey::parse("aabbccddeeff0"), Err(KeyError::BadLength(13)));
        assert_eq!(WifiKey::parse(""), Err(KeyError::BadLength(0)));
        assert_eq!(WifiKey::parse("aabbccddeefg"), Err(KeyError::NotHex('g')));
    }

    #[test]
    fn separated_addresses_are_rejected_so_one_ap_cannot_gain_two_identities() {
        assert!(WifiKey::parse("aa:bb:cc:dd:ee:ff").is_err());
        assert!(WifiKey::parse("aa-bb-cc-dd-ee-ff").is_err());
    }

    #[test]
    fn radio_parsing_accepts_the_spellings_clients_send() {
        assert_eq!(Radio::parse("LTE"), Some(Radio::Lte));
        assert_eq!(Radio::parse("lte"), Some(Radio::Lte));
        assert_eq!(Radio::parse("umts"), Some(Radio::Wcdma));
        assert_eq!(Radio::parse(" wcdma "), Some(Radio::Wcdma));
        assert_eq!(Radio::parse("gsm"), Some(Radio::Gsm));
        assert_eq!(Radio::parse("carrier pigeon"), None);
    }

    #[test]
    fn radio_numbering_matches_upstream_and_must_not_change() {
        assert_eq!(Radio::Gsm.as_i8(), 0);
        assert_eq!(Radio::Cdma.as_i8(), 1);
        assert_eq!(Radio::Wcdma.as_i8(), 2);
        assert_eq!(Radio::Lte.as_i8(), 3);
        for r in [Radio::Gsm, Radio::Cdma, Radio::Wcdma, Radio::Lte] {
            assert_eq!(Radio::from_i8(r.as_i8()), Some(r));
        }
    }

    #[test]
    fn tokens_round_trip_for_both_kinds() {
        let w = StationKey::Wifi(WifiKey::parse("aabbccddeeff").unwrap());
        let c = StationKey::Cell(cell());
        assert_eq!(StationKey::from_token(&w.to_token()), Some(w));
        assert_eq!(StationKey::from_token(&c.to_token()), Some(c));
    }

    #[test]
    fn malformed_tokens_are_rejected_rather_than_guessed_at() {
        assert_eq!(StationKey::from_token("w:short"), None);
        assert_eq!(StationKey::from_token("c:3:432:11:1234"), None); // one field missing
        assert_eq!(StationKey::from_token("c:3:432:11:1234:5:6"), None); // one field extra
        assert_eq!(StationKey::from_token("x:whatever"), None);
        assert_eq!(StationKey::from_token("nocolon"), None);
    }

    #[test]
    fn sharding_is_deterministic_and_in_range() {
        let key = StationKey::Wifi(WifiKey::parse("aabbccddeeff").unwrap());
        let first = key.shard(32);
        for _ in 0..100 {
            assert_eq!(key.shard(32), first);
        }
        assert!(first < 32);
    }

    #[test]
    fn the_shard_hash_is_pinned_so_stations_never_migrate_between_shards() {
        // These values are part of the on-disk contract in the sense that a
        // change would let two workers fold the same station at once during a
        // rolling deploy. Update them only with a planned drain.
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x85944171f73967e8);
    }

    #[test]
    fn sharding_spreads_keys_across_every_shard() {
        let mut counts = vec![0usize; 32];
        for i in 0..20_000u64 {
            let k = StationKey::Wifi(WifiKey(i));
            counts[k.shard(32) as usize] += 1;
        }
        // With 20k keys over 32 shards an even split is 625. Anything inside a
        // factor of two says the hash is not clumping.
        for (shard, &n) in counts.iter().enumerate() {
            assert!(n > 300 && n < 1_200, "shard {shard} got {n} keys");
        }
    }

    #[test]
    fn a_single_shard_puts_everything_in_shard_zero() {
        assert_eq!(StationKey::Wifi(WifiKey(12345)).shard(1), 0);
        assert_eq!(StationKey::Cell(cell()).shard(1), 0);
    }

    #[test]
    fn wifi_and_cell_keys_do_not_collide_in_token_space() {
        let w = StationKey::Wifi(WifiKey::parse("000000000003").unwrap());
        let c = StationKey::Cell(cell());
        assert_ne!(w.to_token(), c.to_token());
        assert_eq!(w.kind(), StationKind::Wifi);
        assert_eq!(c.kind(), StationKind::Cell);
    }
}
