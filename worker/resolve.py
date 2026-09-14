"""A resident yt-dlp, so a resolve costs the extraction and not the interpreter.

Spawning `yt-dlp` per request spends about 840ms before it has looked at anything: Python starting
and yt-dlp importing itself. Measured on the box, that is roughly two fifths of every resolve, and it
is paid again for every trailer. This process pays it once and then answers over a pipe.

Deliberately NOT a pip install. yt-dlp arrives as the same SHA-pinned zipapp the image already
verifies, put on `sys.path` and imported — the supply-chain guard is the whole point of pinning it,
and a resident process is not worth giving that up for.

The protocol is one JSON object per line in, one per line out, because that is all it needs to be:

    {"id": "dQw4w9WgXcQ", "format": "bv*[...]+ba[...]"}
    {"ok": true, "width": 1920, "height": 1080, "urls": [...], "hls": "https://..."}
    {"ok": false, "error": "ERROR: [youtube] ... Video unavailable"}

A failure is reported, never raised: the caller maps yt-dlp's own words onto an HTTP status exactly
as it does for the subprocess, so both paths fail identically. Anything that escapes that and kills
this process is fine too — the caller notices the pipe close and falls back to spawning the binary.
"""

import json
import os
import sys

# The zipapp is a zip of the package; importing from it is what lets the pinned artifact be reused.
# `YTDLP_ZIP` overrides where it is, so this can be run outside the image it is built for.
sys.path.insert(0, os.environ.get("YTDLP_ZIP", "/usr/local/lib/yt-dlp.zip"))

from yt_dlp import YoutubeDL  # noqa: E402  (only importable once the path above is set)
from yt_dlp.utils import DownloadError  # noqa: E402


def answer(request):
    """Resolve one id into the same three things `/direct` returns."""
    options = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "socket_timeout": 15,
        "format": request["format"],
        "cachedir": request.get("cache") or False,
    }
    if request.get("extractor_args"):
        # Same shape as the CLI's `--extractor-args youtube:player_client=...`.
        key, _, value = request["extractor_args"].partition(":")
        options["extractor_args"] = {key: dict(p.split("=", 1) for p in value.split(";") if "=" in p)}

    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(
            "https://www.youtube.com/watch?v=" + request["id"], download=False
        )

    # What the CLI's `--print urls` prints: the selected formats, in the order the selector named
    # them (video then audio), or the one format when the selection was already muxed.
    chosen = info.get("requested_formats") or [info]
    urls = [f["url"] for f in chosen if f.get("url")]
    # Every HLS format names the same master playlist, and the progressive selection above carries
    # none — so this is the only way one resolve answers both transports.
    hls = next(
        (f["manifest_url"] for f in info.get("formats") or [] if f.get("manifest_url")), None
    )
    return {
        "ok": True,
        "width": info.get("width"),
        "height": info.get("height"),
        "urls": urls,
        "hls": hls,
    }


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            reply = answer(json.loads(line))
        except DownloadError as failure:
            # yt-dlp's own message, so the caller can classify it the way it classifies stderr.
            reply = {"ok": False, "error": str(failure)}
        except Exception as failure:  # noqa: BLE001 — one bad id must not end the process
            reply = {"ok": False, "error": "%s: %s" % (type(failure).__name__, failure)}
        sys.stdout.write(json.dumps(reply) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
