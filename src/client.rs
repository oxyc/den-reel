//! A browser's report of what it plays, so a trailer's master playlist lists only what that browser can play.
//!
//! Den Web measures it once (den-edge `web/src/lib/playable.ts`) and sends the same report to den-remux and
//! den-scout. It arrives here either way a page can send it: the `X-Den-Playable` header, where hls.js fetches the
//! master and can set one, or a `playable` query parameter carrying the same JSON, for Safari's own player, which
//! fetches a bare URL and sends no header the page chose. Without either, the master lists everything, as before.
//!
//! It matters most for Safari's own player, which picks its variant for itself: the playlist is the only lever on
//! it, so a variant it can't decode has to be left out rather than merely listed last.

use hyper::header::HeaderMap;
use serde::Deserialize;

pub const HEADER: &str = "x-den-playable";
const PARAM: &str = "playable";

/// The report is about 300 bytes; anything much longer is not one.
const MAX_REPORT_BYTES: usize = 2048;

/// The fields of the report that decide a video variant. Levels are the codecs' own numbers — H.264 `level_idc`,
/// HEVC `general_level_idc` (level × 30), AV1 `seq_level_idx` — and 0 means none. The audio fields but E-AC-3 are
/// not read: every variant YouTube publishes carries AAC.
#[derive(Deserialize, Clone, Copy, Debug, Default, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct Playable {
    pub h264: u16,
    pub h264_high10: u16,
    pub hevc_main: u16,
    pub hevc_main10: u16,
    pub hevc_high_tier: u16,
    pub hdr: bool,
    pub eac3: bool,
    pub dolby_vision: DolbyVision,
    pub av1: u16,
    pub av1_main10: u16,
    pub av1_hdr: bool,
    pub vp9: bool,
    pub vp9_profile2: bool,
}

#[derive(Deserialize, Clone, Copy, Debug, Default, PartialEq)]
#[serde(default)]
pub struct DolbyVision {
    pub p5: bool,
    pub p8: bool,
}

/// The report a request carries: the header where there is one, else the query parameter. `None` when neither is
/// there or what is there doesn't parse — a master listing every variant is a worse answer for that browser, not a
/// wrong one.
pub fn from_request(headers: &HeaderMap, query: &str) -> Option<Playable> {
    let raw = headers
        .get(HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| crate::httputil::query_param(query, PARAM))?;
    let parsed = (raw.len() <= MAX_REPORT_BYTES).then(|| serde_json::from_str(&raw).ok()).flatten();
    if parsed.is_none() {
        crate::log_limited("playable report", || {
            "hls: a playable report that does not parse; listing every variant".to_string()
        });
    }
    parsed
}

impl Playable {
    /// Whether this player decodes a variant: every codec its `CODECS` names, at its profile, level and tier, with
    /// the HDR its `VIDEO-RANGE` names. A codec the report has no field for — AAC, and anything unrecognised — is
    /// taken, as is one whose string is too short to read: refusing what nothing measured would refuse variants
    /// that play.
    pub fn takes(&self, codecs: &str, video_range: Option<&str>) -> bool {
        let hdr = matches!(video_range, Some("PQ" | "HLG"));
        codecs.split(',').map(str::trim).all(|codec| self.takes_codec(codec, hdr))
    }

    fn takes_codec(&self, codec: &str, hdr: bool) -> bool {
        let mut parts = codec.split('.');
        match parts.next().unwrap_or("") {
            // avc1.PPCCLL: profile_idc, constraint flags and level_idc, in hex. High 10 is its own decoder.
            "avc1" | "avc3" => {
                let Some(hex) = parts.next().filter(|h| h.len() == 6) else { return true };
                let (Ok(profile), Ok(level)) =
                    (u8::from_str_radix(&hex[..2], 16), u16::from_str_radix(&hex[4..], 16))
                else {
                    return true;
                };
                let most = if profile == 110 { self.h264_high10 } else { self.h264 };
                most > 0 && level <= most
            }
            // hvc1.P.C.TLL.…: profile (after an optional profile-space letter), compatibility flags, then tier
            // (L main, H high) and general_level_idc.
            "hvc1" | "hev1" => {
                let profile = parts.next().map(|p| p.trim_start_matches(['A', 'B', 'C']));
                let Some(tier_level) = parts.nth(1) else { return true };
                let Ok(level) = tier_level.get(1..).unwrap_or("").parse::<u16>() else { return true };
                let most = match (tier_level.starts_with('H'), profile) {
                    (true, _) => self.hevc_high_tier,
                    (false, Some("2")) => self.hevc_main10,
                    _ => self.hevc_main.max(self.hevc_main10),
                };
                most > 0 && level <= most && (!hdr || self.hdr)
            }
            // dvh1.PP.LL: Dolby Vision's own profile.
            "dvh1" | "dvhe" => match parts.next() {
                Some("05") => self.dolby_vision.p5,
                Some("08") => self.dolby_vision.p8,
                _ => false,
            },
            // av01.P.LLT.DD: profile, seq_level_idx and tier, bit depth. A player is only asked about Main profile
            // and tier.
            "av01" => {
                let (Some(profile), Some(level_tier)) = (parts.next(), parts.next()) else { return true };
                let Ok(level) = level_tier.get(..2).unwrap_or("").parse::<u16>() else { return true };
                if profile != "0" || !level_tier.ends_with('M') {
                    return false;
                }
                let most = match parts.next() {
                    Some("10") => self.av1_main10,
                    _ => self.av1.max(self.av1_main10),
                };
                most > 0 && level <= most && (!hdr || self.av1_hdr)
            }
            // vp09.PP.LL.DD: profile 0 is 8-bit, profile 2 is 10-bit; nothing asks about 1 or 3.
            "vp09" => match parts.next() {
                None => self.vp9 || self.vp9_profile2,
                Some("00") => self.vp9,
                Some("02") => self.vp9_profile2,
                Some(_) => false,
            },
            "ec-3" | "ac-3" => self.eac3,
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Chrome on a Mac, roughly: 4K H.264 and HEVC with HDR, AV1, VP9 through hls.js, no Dolby Vision or E-AC-3.
    fn chrome() -> Playable {
        Playable {
            h264: 51,
            hevc_main: 153,
            hevc_main10: 153,
            hdr: true,
            av1: 13,
            av1_main10: 13,
            av1_hdr: true,
            vp9: true,
            vp9_profile2: true,
            ..Playable::default()
        }
    }

    #[test]
    fn the_report_comes_from_the_header_or_else_the_query() {
        let json = r#"{"h264":51,"vp9":true,"aacMultichannel":true}"#;
        let mut headers = HeaderMap::new();
        headers.insert(HEADER, json.parse().unwrap());
        let from_header = from_request(&headers, "").expect("a header report");
        assert_eq!((from_header.h264, from_header.vp9), (51, true));

        let query = format!("s=tag&playable={}", "%7B%22h264%22%3A41%7D");
        assert_eq!(from_request(&HeaderMap::new(), &query).map(|p| p.h264), Some(41), "percent-encoded JSON");
        assert_eq!(
            from_request(&headers, &query).map(|p| p.h264),
            Some(51),
            "the header wins where both are sent"
        );

        assert_eq!(from_request(&HeaderMap::new(), "s=tag"), None, "no report");
        assert_eq!(from_request(&HeaderMap::new(), "playable=%7Bnot%20json"), None, "one that doesn't parse");
        let long = format!("playable={}", "x".repeat(MAX_REPORT_BYTES + 1));
        assert_eq!(from_request(&HeaderMap::new(), &long), None, "one far too long to be a report");
    }

    #[test]
    fn a_variant_plays_when_every_codec_it_names_does() {
        let p = chrome();
        assert!(p.takes("avc1.640028,mp4a.40.2", None));
        assert!(!p.takes("avc1.640034,mp4a.40.2", None), "H.264 level 5.2 is past 5.1");
        assert!(!p.takes("avc1.6E0028", None), "High 10 is its own decoder");
        assert!(p.takes("hvc1.2.4.L150.B0", Some("PQ")));
        assert!(!p.takes("hvc1.2.4.H150.B0", None), "high tier, which the report doesn't take");
        assert!(!Playable { hdr: false, ..p }.takes("hvc1.2.4.L150.B0", Some("PQ")), "HDR without HDR");
        assert!(p.takes("av01.0.12M.10.0.110.09.16.09.0", Some("PQ")));
        assert!(!p.takes("av01.0.14M.08", None), "past AV1's level");
        assert!(!p.takes("av01.0.12H.10", None), "AV1 high tier");
        assert!(p.takes("vp09.00.40.08,mp4a.40.2", None));
        assert!(!Playable { vp9: false, ..p }.takes("vp09.00.40.08,mp4a.40.2", None), "no VP9 profile 0");
        assert!(!p.takes("vp09.01.40.08", None), "VP9 profile 1");
        assert!(!p.takes("dvh1.08.06", None), "Dolby Vision it doesn't show");
        assert!(!p.takes("avc1.640028,ec-3", None), "E-AC-3 it doesn't play");
        assert!(p.takes("mp4a.40.2", None) && p.takes("opus", None), "audio it has no field for");
        assert!(p.takes("avc1", None) && p.takes("hvc1.2", None), "strings too short to read");
    }
}
