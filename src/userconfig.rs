//! Per-install config, base64url-encoded into the addon URL path (den-scout/den-subtitles style). It
//! carries the user's BYOK TMDB key (and an optional KinoCheck key) — the discovery credentials that
//! used to live in the server environment. It is a **bearer secret**: the Den app builds it at
//! `/configure`, seals it to the addon's key, stores it in the Keychain, and never logs it. We validate
//! + bound the untrusted blob before use and never echo the key back.

use base64::Engine;
use serde::Deserialize;

/// A validated install config. Both keys are BYOK and ride in the addon URL. The TMDB key (trailer
/// discovery) is required; the KinoCheck key (fallback source) is optional.
#[derive(Debug, Clone)]
pub struct UserConfig {
    pub tmdb_key: String,
    pub kinocheck_key: Option<String>,
}

/// Untrusted wire shape before validation.
#[derive(Deserialize)]
struct RawConfig {
    #[serde(rename = "tmdbKey", default)]
    tmdb_key: String,
    #[serde(rename = "kinocheckKey")]
    kinocheck_key: Option<String>,
}

/// Decode the config path segment into a validated config, or `None` (→ 400). The decoded bytes are
/// either a SEALED blob (first byte == `SEALED_VERSION` → decrypt with the keyring) or a legacy plaintext
/// JSON config (first byte `{`). Sealed with no keyring, or a decrypt failure, fails CLOSED — never a
/// partial/empty config. Mirrors den-scout/den-subtitles (den-scout/docs/SEALED-CONFIG.md).
pub fn decode(keyring: Option<&crate::seal::Keyring>, blob: &str) -> Option<UserConfig> {
    let data = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(blob).ok()?;
    let data = if data.first() == Some(&crate::seal::SEALED_VERSION) {
        keyring?.open(&data[1..])? // sealed but no key, or decrypt fail → None
    } else {
        data // legacy plaintext
    };
    let raw: RawConfig = serde_json::from_slice(&data).ok()?;
    validate(raw)
}

fn validate(raw: RawConfig) -> Option<UserConfig> {
    // The TMDB key is the discovery credential — required, bounded. (TMDB v3 keys are 32 hex chars;
    // v4 read tokens are longer JWTs. Accept a generous range so either works without pinning a format.)
    if raw.tmdb_key.is_empty() || raw.tmdb_key.len() > 512 {
        return None;
    }
    let kinocheck_key = raw.kinocheck_key.filter(|k| !k.is_empty() && k.len() <= 256);
    Some(UserConfig { tmdb_key: raw.tmdb_key, kinocheck_key })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(json: &str) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
    }

    #[test]
    fn decodes_a_plaintext_config() {
        let cfg = decode(None, &encode(r#"{"tmdbKey":"abc123"}"#)).unwrap();
        assert_eq!(cfg.tmdb_key, "abc123");
        assert!(cfg.kinocheck_key.is_none());
    }

    #[test]
    fn carries_an_optional_kinocheck_key() {
        let cfg = decode(None, &encode(r#"{"tmdbKey":"abc","kinocheckKey":"kc"}"#)).unwrap();
        assert_eq!(cfg.kinocheck_key.as_deref(), Some("kc"));
    }

    #[test]
    fn rejects_a_config_with_no_tmdb_key() {
        assert!(decode(None, &encode(r#"{"kinocheckKey":"kc"}"#)).is_none());
        assert!(decode(None, &encode(r#"{"tmdbKey":""}"#)).is_none());
        assert!(decode(None, "not base64!!").is_none());
    }

    #[test]
    fn decodes_a_sealed_config() {
        // A fixed segment sealing {tmdbKey, kinocheckKey} to the vector key with real libsodium
        // (PyNaCl SealedBox) — the sealed→UserConfig gate for den-reel, byte-compatible with the Go
        // addon, den-subtitles, and the browser bundle (same wire format, same vector key).
        const VEC_PRIV: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        const SEG: &str = "Abo-qmntVxuOmeVa0Q5pPWju0VrZDS4aRoAP-0JHNtk7nmMcduhttWlvldwvUdXPafUGUegc4ul5J3gFVo8nEGOd8htc7he_3BihPsWtiuA5_2Du-FL5NpaNzfvqhDAHM_LAjw";
        let kr = crate::seal::Keyring::from_env(VEC_PRIV, "").unwrap().unwrap();

        let cfg = decode(Some(&kr), SEG).expect("sealed segment decodes");
        assert_eq!(cfg.tmdb_key, "sealed-tmdb-ok");
        assert_eq!(cfg.kinocheck_key.as_deref(), Some("kc-ok"));

        // Fail CLOSED: the same sealed segment with no keyring configured.
        assert!(decode(None, SEG).is_none());
        // Back-compat: legacy plaintext still decodes with a keyring present.
        assert!(decode(Some(&kr), &encode(r#"{"tmdbKey":"legacy"}"#)).is_some());
    }
}
