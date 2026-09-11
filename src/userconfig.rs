//! Per-install config, base64url-encoded into the addon URL path (den-scout/den-subtitles style). It
//! carries the user's BYOK TMDB key (and an optional KinoCheck key) — the discovery credentials that
//! used to live in the server environment. It is a **bearer secret**: the Den app builds it at
//! `/configure`, seals it to the addon's key, stores it in the Keychain, and never logs it. We validate
//! + bound the untrusted blob before use and never echo the key back.

use std::collections::HashSet;

use base64::Engine;
use serde::Deserialize;

/// A validated install config. Both keys are BYOK and ride in the addon URL. The TMDB key (trailer
/// discovery) is required; the KinoCheck key (fallback source) is optional.
#[derive(Debug, Clone)]
pub struct UserConfig {
    pub tmdb_key: String,
    pub kinocheck_key: Option<String>,
    /// Install id (`iid`), minted by /configure for each link it builds: what `REVOKED_INSTALLS`
    /// names. `None` on a link built before ids existed, which only `CONFIG_EPOCH` can revoke.
    pub iid: Option<String>,
    /// The config epoch (`ep`) the link was stamped with; absent reads as 0.
    pub ep: u64,
}

/// Untrusted wire shape before validation.
#[derive(Deserialize)]
struct RawConfig {
    #[serde(rename = "tmdbKey", default)]
    tmdb_key: String,
    #[serde(rename = "kinocheckKey")]
    kinocheck_key: Option<String>,
    iid: Option<String>,
    ep: Option<u64>,
}

/// An install id as /configure mints it: 16 random bytes, base64url, unpadded — 22 characters. The
/// engine refuses padding and non-zero trailing bits, so each id has exactly one spelling and a
/// `REVOKED_INSTALLS` entry cannot be dodged by re-encoding the same bytes.
fn is_install_id(s: &str) -> bool {
    s.len() == 22 && base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s).is_ok_and(|b| b.len() == 16)
}

/// Which installs are refused (issue #8 R3). Naming an install id kills one leaked link; raising the
/// epoch kills every link stamped before it. Neither touches the sealing key — and rotating that key
/// would not do this, since `CONFIG_KEYS_PREV` keeps links sealed to the old key opening on purpose.
#[derive(Debug, Default)]
pub struct Revocation {
    revoked: HashSet<String>,
    epoch: u64,
    /// `REQUIRE_INSTALL_ID`: refuse a config with no install id — every link built before ids
    /// existed. Those can't be named, and raising the epoch would also refuse the links minted since,
    /// which carry the same epoch.
    require_iid: bool,
}

impl Revocation {
    /// From `REVOKED_INSTALLS` (comma-separated install ids) and `CONFIG_EPOCH` (default 0). A
    /// malformed entry is skipped with a warning: no config that decodes can carry it, so it could
    /// never match. An unparseable epoch is said loudly and enforced as 0, and the startup line's
    /// `epoch=` shows the value actually in force.
    pub fn from_env(revoked: &str, epoch: Option<&str>) -> Revocation {
        let mut ids = HashSet::new();
        for entry in revoked.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            if is_install_id(entry) {
                ids.insert(entry.to_string());
            } else {
                eprintln!("warning: skipping a REVOKED_INSTALLS entry that is not a 22-character install id");
            }
        }
        let epoch = match epoch.map(str::trim).filter(|e| !e.is_empty()) {
            None => 0,
            Some(raw) => raw.parse().unwrap_or_else(|_| {
                eprintln!("warning: CONFIG_EPOCH={raw:?} is not a non-negative integer — enforcing epoch 0");
                0
            }),
        };
        Revocation { revoked: ids, epoch, require_iid: false }
    }

    /// Also refuse configs that carry no install id (`REQUIRE_INSTALL_ID`).
    pub fn requiring_install_id(mut self, on: bool) -> Revocation {
        self.require_iid = on;
        self
    }

    pub fn revoked_count(&self) -> usize {
        self.revoked.len()
    }

    pub fn requires_install_id(&self) -> bool {
        self.require_iid
    }

    /// The oldest epoch still admitted — what /configure stamps into a new link.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether `cfg`'s install is still admitted. The id is checked first, as the more specific answer.
    fn check(&self, cfg: &UserConfig) -> Result<(), Rejected> {
        if let Some(iid) = cfg.iid.as_deref().filter(|iid| self.revoked.contains(*iid)) {
            return Err(Rejected::Revoked { iid_prefix: iid.chars().take(6).collect() });
        }
        if cfg.ep < self.epoch {
            return Err(Rejected::EpochTooOld { ep: cfg.ep, epoch: self.epoch });
        }
        if self.require_iid && cfg.iid.is_none() {
            return Err(Rejected::NoInstallId);
        }
        Ok(())
    }
}

/// Why a config segment was not accepted. Every variant gets the same answer on the wire; the
/// difference is for the log only, and nothing here is a credential: a revoked id is kept to its
/// first six characters, and an admitted install's id never gets this far.
#[derive(Debug, PartialEq, Eq)]
pub enum Rejected {
    Undecodable,
    Revoked { iid_prefix: String },
    EpochTooOld { ep: u64, epoch: u64 },
    NoInstallId,
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rejected::Undecodable => f.write_str("config does not decode"),
            Rejected::Revoked { iid_prefix } => write!(f, "install revoked (iid={iid_prefix}…)"),
            Rejected::EpochTooOld { ep, epoch } => {
                write!(f, "install epoch too old (ep={ep} < CONFIG_EPOCH={epoch})")
            }
            Rejected::NoInstallId => f.write_str("no install id (REQUIRE_INSTALL_ID is on)"),
        }
    }
}

/// `decode`, then the revocation check. The config-scoped routes read their config through this
/// and nothing else, so a revocation reaches every URL that embeds the config.
pub fn decode_checked(
    keyring: Option<&crate::seal::Keyring>,
    revocation: &Revocation,
    blob: &str,
) -> Result<UserConfig, Rejected> {
    let cfg = decode(keyring, blob).ok_or(Rejected::Undecodable)?;
    revocation.check(&cfg)?;
    Ok(cfg)
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
    // A malformed install id is a malformed config: admitting it would leave an install that no
    // `REVOKED_INSTALLS` entry can ever name.
    if raw.iid.as_deref().is_some_and(|iid| !is_install_id(iid)) {
        return None;
    }
    // The TMDB key is the discovery credential — required, bounded. (TMDB v3 keys are 32 hex chars;
    // v4 read tokens are longer JWTs. Accept a generous range so either works without pinning a format.)
    if raw.tmdb_key.is_empty() || raw.tmdb_key.len() > 512 {
        return None;
    }
    let kinocheck_key = raw.kinocheck_key.filter(|k| !k.is_empty() && k.len() <= 256);
    Some(UserConfig { tmdb_key: raw.tmdb_key, kinocheck_key, iid: raw.iid, ep: raw.ep.unwrap_or(0) })
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

    /// Bytes 0..16 as an install id, and a second one that uses both url-safe characters.
    const IID: &str = "AAECAwQFBgcICQoLDA0ODw";
    const OTHER_IID: &str = "_-_-_-_-_-_-_-_-_-_-_w";

    #[test]
    fn install_ids_are_validated_strictly() {
        for ok in [IID, OTHER_IID] {
            assert!(is_install_id(ok), "refused {ok}");
        }
        let refused = [
            "",
            "AAECAwQFBgcICQoLDA0OD",    // 21 characters
            "AAECAwQFBgcICQoLDA0ODwA",  // 23 characters
            "AAECAwQFBgcICQoLDA0ODw==", // padded
            "AAECAwQFBgcICQoLDA0ODx",   // the same bytes with a non-zero trailing bit
            "AAECAwQFBgcICQoLDA0O+w",   // standard base64, not url-safe
            "AAECAwQFBgcICQoLDA0O/w",
            "AAECAwQFBgcICQoLDA0O w",
        ];
        for bad in refused {
            assert!(!is_install_id(bad), "accepted {bad:?}");
        }
    }

    #[test]
    fn a_config_carries_its_install_id_and_epoch() {
        let cfg = decode(None, &encode(&format!(r#"{{"tmdbKey":"k","iid":"{IID}","ep":3}}"#))).unwrap();
        assert_eq!(cfg.iid.as_deref(), Some(IID));
        assert_eq!(cfg.ep, 3);
        // Absent: no id, epoch 0.
        let cfg = decode(None, &encode(r#"{"tmdbKey":"k"}"#)).unwrap();
        assert!(cfg.iid.is_none());
        assert_eq!(cfg.ep, 0);
        // A malformed id or epoch makes the whole config malformed.
        for bad in [
            r#"{"tmdbKey":"k","iid":"short"}"#,
            r#"{"tmdbKey":"k","iid":"AAECAwQFBgcICQoLDA0ODw=="}"#,
            r#"{"tmdbKey":"k","iid":7}"#,
            r#"{"tmdbKey":"k","ep":-1}"#,
            r#"{"tmdbKey":"k","ep":1.5}"#,
            r#"{"tmdbKey":"k","ep":"1"}"#,
        ] {
            assert!(decode(None, &encode(bad)).is_none(), "accepted {bad}");
        }
    }

    #[test]
    fn a_listed_install_and_an_old_epoch_are_refused() {
        let revocation = Revocation::from_env(&format!(" {IID} ,, not-an-id"), Some("2"));
        assert_eq!(revocation.revoked_count(), 1, "the malformed entry is skipped");
        assert_eq!(revocation.epoch(), 2);
        let check = |json: &str| decode_checked(None, &revocation, &encode(json));

        let listed = format!(r#"{{"tmdbKey":"k","iid":"{IID}","ep":5}}"#);
        assert_eq!(check(&listed).unwrap_err(), Rejected::Revoked { iid_prefix: "AAECAw".into() });
        assert!(check(&format!(r#"{{"tmdbKey":"k","iid":"{OTHER_IID}","ep":5}}"#)).is_ok());

        assert_eq!(
            check(r#"{"tmdbKey":"k","ep":1}"#).unwrap_err(),
            Rejected::EpochTooOld { ep: 1, epoch: 2 }
        );
        assert_eq!(check(r#"{"tmdbKey":"k"}"#).unwrap_err(), Rejected::EpochTooOld { ep: 0, epoch: 2 });
        assert!(check(r#"{"tmdbKey":"k","ep":2}"#).is_ok());

        // Nothing configured: a link with no id and no epoch is admitted, as every link before ids was.
        assert!(decode_checked(None, &Revocation::default(), &encode(r#"{"tmdbKey":"k"}"#)).is_ok());
        assert_eq!(decode_checked(None, &revocation, "!!").unwrap_err(), Rejected::Undecodable);
    }

    #[test]
    fn a_refusal_names_no_more_than_the_revoked_id_s_prefix() {
        assert_eq!(
            Rejected::Revoked { iid_prefix: "AAECAw".into() }.to_string(),
            "install revoked (iid=AAECAw…)"
        );
        assert_eq!(
            Rejected::EpochTooOld { ep: 1, epoch: 2 }.to_string(),
            "install epoch too old (ep=1 < CONFIG_EPOCH=2)"
        );
    }

    #[test]
    fn requiring_ids_refuses_only_links_without_one() {
        let revocation = Revocation::from_env("", None).requiring_install_id(true);
        assert!(revocation.requires_install_id());
        let check = |json: &str| decode_checked(None, &revocation, &encode(json));
        assert_eq!(check(r#"{"tmdbKey":"k"}"#).unwrap_err(), Rejected::NoInstallId);
        assert!(check(&format!(r#"{{"tmdbKey":"k","iid":"{IID}","ep":0}}"#)).is_ok());
        assert_eq!(Rejected::NoInstallId.to_string(), "no install id (REQUIRE_INSTALL_ID is on)");
        // Off by default: links from before ids keep working until the operator turns it on.
        assert!(!Revocation::from_env("", None).requires_install_id());
    }

    #[test]
    fn an_unparseable_epoch_enforces_zero() {
        assert_eq!(Revocation::from_env("", None).epoch(), 0);
        assert_eq!(Revocation::from_env("", Some("two")).epoch(), 0);
        assert_eq!(Revocation::from_env("", Some("-1")).epoch(), 0);
        assert_eq!(Revocation::from_env("", Some(" 7 ")).epoch(), 7);
    }
}
