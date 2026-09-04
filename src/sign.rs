//! Optional signing of the URLs that cost a download.
//!
//! `/play` and `/crop` both take a YouTube id, spend one of three download permits and a yt-dlp
//! process on it, and cache the result on the operator's volume. The only check in front of either
//! is [`crate::is_valid_vid`], so an instance reachable from outside the LAN — which is exactly what
//! the README's `https://trailers.<domain>` deployment is — is a YouTube extraction service anyone
//! can point at any video, and a way to fill a 4 GB cache with things nobody asked for.
//!
//! With `REEL_PLAY_SECRET` set, `/meta` hands out `…/play/<vid>.mp4?s=<tag>` and both endpoints
//! require a tag that verifies. Unset, nothing changes at all: this has to default off because
//! `/meta` ships `max-age=604800`, so clients hold unsigned play URLs for up to a week and turning
//! signing on unconditionally would break every install for that week.
//!
//! The tag covers the **id alone**, deliberately, not the path or an expiry. Not the path, so a
//! client can carry the `s` it was given on the play URL straight over to `/crop` for the same id —
//! the two endpoints authorise the same work. Not an expiry, because the play URL is immutable and
//! cached hard by design (`max-age=31536000`), and an expiring URL inside an immutable response is
//! a broken trailer waiting for a clock to tick over.
//!
//! Keyed BLAKE2b, from the `blake2` crate already in the tree (crypto_box uses it for the seal
//! nonce), so this adds no compiled code — only a direct dependency edge.

use blake2::digest::{consts::U12, Mac};
use blake2::{Blake2b512, Blake2bMac, Digest};
use subtle::ConstantTimeEq;

/// 12 bytes → 24 hex characters. Long enough that guessing is hopeless, short enough not to bloat a
/// URL that ends up in a manifest, a client cache and a log line.
type Tag = Blake2bMac<U12>;

/// 12 bytes of tag, hex-encoded.
const TAG_HEX_LEN: usize = 24;

/// The MAC key, derived from the configured secret rather than used raw: BLAKE2b's key is capped at
/// 64 bytes, and a secret is whatever the operator typed. Hashing first accepts any length without
/// making the length a configuration error.
fn key_of(secret: &str) -> [u8; 32] {
    let mut h = Blake2b512::new();
    h.update(secret.as_bytes());
    let out = h.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&out[..32]);
    key
}

/// The tag for `vid`, lowercase hex.
pub fn tag(secret: &str, vid: &str) -> String {
    let key = key_of(secret);
    let mut mac = <Tag as Mac>::new_from_slice(&key).expect("32 bytes is a valid BLAKE2b key");
    mac.update(vid.as_bytes());
    let out = mac.finalize().into_bytes();
    let mut s = String::with_capacity(out.len() * 2);
    for b in out {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap_or('0'));
    }
    s
}

/// Does `presented` match the tag for `vid`? Constant-time, so a caller cannot learn the tag one
/// character at a time from how long the comparison took.
pub fn verify(secret: &str, vid: &str, presented: Option<&str>) -> bool {
    let Some(presented) = presented else { return false };
    // Length first, before deriving anything. The length is public — it is a fixed 24 either way, so
    // checking it early leaks nothing — and it means junk costs a comparison rather than two BLAKE2b
    // key schedules per configured secret.
    if presented.len() != TAG_HEX_LEN {
        return false;
    }
    let expected = tag(secret, vid);
    presented.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// Verify against the current secret, then against any prior one still in rotation.
///
/// Rotating the secret would otherwise 403 every play URL a client is holding — and `/meta` tells
/// clients to hold them for a week — so a rotation without this is a week-long outage. Exactly the
/// problem `REEL_CONFIG_KEYS_PREV` already exists to solve for the sealing key.
///
/// The scan is not constant-time *across* the set, only within each comparison. What that leaks is
/// how many secrets are configured, which is not a secret.
pub fn verify_any(current: &str, prev: &[String], vid: &str, presented: Option<&str>) -> bool {
    verify(current, vid, presented) || prev.iter().any(|s| verify(s, vid, presented))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tag_is_stable_and_specific() {
        let a = tag("s3cret", "dSdWpY2Bxsc");
        assert_eq!(a, tag("s3cret", "dSdWpY2Bxsc"), "the same input must tag the same way");
        assert_ne!(a, tag("s3cret", "dQw4w9WgXcQ"), "the tag must cover the id");
        assert_ne!(a, tag("other", "dSdWpY2Bxsc"), "the tag must cover the secret");
        assert_eq!(a.len(), 24, "24 hex characters");
        assert!(a.bytes().all(|b| b.is_ascii_hexdigit()), "hex only: it goes in a URL");
    }

    #[test]
    fn verify_accepts_only_the_real_tag() {
        let good = tag("s3cret", "dSdWpY2Bxsc");
        assert!(verify("s3cret", "dSdWpY2Bxsc", Some(&good)));
        assert!(!verify("s3cret", "dSdWpY2Bxsc", None), "no tag is not a pass");
        assert!(!verify("s3cret", "dSdWpY2Bxsc", Some("")), "an empty tag is not a pass");
        assert!(!verify("s3cret", "dQw4w9WgXcQ", Some(&good)), "a tag for one id must not open another");
        assert!(!verify("other", "dSdWpY2Bxsc", Some(&good)), "a tag from another secret must not pass");
        // Truncation must not pass either — the length check is part of the comparison.
        assert!(!verify("s3cret", "dSdWpY2Bxsc", Some(&good[..8])));
    }

    /// A rotated secret must not 403 the play URLs clients were told to cache for a week.
    #[test]
    fn a_prior_secret_still_verifies() {
        let old_tag = tag("old-secret", "dSdWpY2Bxsc");
        let prev = vec!["old-secret".to_string()];

        assert!(!verify("new-secret", "dSdWpY2Bxsc", Some(&old_tag)), "the premise: it does not verify alone");
        assert!(verify_any("new-secret", &prev, "dSdWpY2Bxsc", Some(&old_tag)), "rotation costs a week of 403s");
        assert!(
            verify_any("new-secret", &prev, "dSdWpY2Bxsc", Some(&tag("new-secret", "dSdWpY2Bxsc"))),
            "the current secret must still be the one that signs"
        );
        assert!(!verify_any("new-secret", &prev, "dSdWpY2Bxsc", Some("deadbeefdeadbeefdeadbeef")));
        assert!(!verify_any("new-secret", &[], "dSdWpY2Bxsc", Some(&old_tag)), "an empty rotation set accepts nothing extra");
    }

    /// A secret longer than BLAKE2b's 64-byte key limit must be usable, not a startup error.
    #[test]
    fn a_long_secret_is_accepted() {
        let long = "x".repeat(500);
        let t = tag(&long, "dSdWpY2Bxsc");
        assert!(verify(&long, "dSdWpY2Bxsc", Some(&t)));
    }
}
