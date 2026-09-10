# den-reel — Rust binary + yt-dlp + ffmpeg, in one slim image.
#
# Three stages: build the Rust binary, fetch the extractor tools (deno + yt-dlp) with curl/unzip in
# a throwaway stage, then assemble a runtime image that carries neither the Rust toolchain nor
# curl/unzip — just ffmpeg, ca-certs, the two extractor binaries, and our ~2 MB binary. No Node, no
# npm, no python3 (yt-dlp's standalone build bundles its own interpreter). Builds amd64, the box's arch.

# ---- build ----------------------------------------------------------------
FROM rust:1-bookworm AS build
WORKDIR /src
# Cache deps: build against manifests + a dummy main first, so a code-only change re-runs only the
# final (LTO'd) link of our crate, not the whole dependency compile.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked   # `strip = true` in the release profile

# ---- build MP4Box (GPAC) — writes the clap box; gpac is gone from Debian repos ----------
# Plain default build → MP4Box + libgpac.so (~10 MB total), linking only libc/libm/libz. Copying the
# two artifacts keeps the runtime debian-slim instead of pulling gpac's ~200-package apt tree.
FROM debian:bookworm-slim AS mp4box
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential zlib1g-dev git ca-certificates && rm -rf /var/lib/apt/lists/*
ARG GPAC_VERSION=v2.4.0
# The commit that tag named when it was pinned. A tag can be moved and a commit cannot, and this source
# is compiled into the image with nothing else checking it — the same reason yt-dlp and deno are
# checksummed below.
ARG GPAC_COMMIT=5d70253ac94e5840be7b86054131dd753af63cc7
RUN git clone --depth 1 --branch ${GPAC_VERSION} https://github.com/gpac/gpac.git /gpac \
    && test "$(git -C /gpac rev-parse HEAD)" = "${GPAC_COMMIT}" \
    && cd /gpac && ./configure && make -j"$(nproc)"

# ---- fetch extractor tools (curl/unzip stay OUT of the runtime image) ------
FROM debian:bookworm-slim AS tools
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# JS runtime for yt-dlp. Recent yt-dlp REQUIRES one to solve YouTube's signature/nsig challenge —
# without it extraction is deprecated, formats go missing, and playback fails intermittently. deno
# is the one yt-dlp enables by default.
ARG DENO_VERSION=2.9.6
# Checksummed like yt-dlp below: this binary executes YouTube's JS in our container, so it is the
# last thing that should arrive unverified.
ARG DENO_SHA256_AMD64=394f07f4da2bebe6ce6f1e7ce0fa16429b29b08c35e3fac3fe25972676dff4b2
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) arch=x86_64-unknown-linux-gnu; sha=$DENO_SHA256_AMD64 ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/denoland/deno/releases/download/v${DENO_VERSION}/deno-${arch}.zip" \
      -o /tmp/deno.zip; \
    echo "${sha}  /tmp/deno.zip" | sha256sum -c -; \
    unzip -q -d /usr/local/bin /tmp/deno.zip

# Pinned yt-dlp STANDALONE binary (PyInstaller onefile — bundles Python, so no system python3
# needed). Bump YTDLP_VERSION to update (YouTube changes often — that's the whole maintenance
# burden, and it's yt-dlp's, not ours).
ARG YTDLP_VERSION=2026.08.19
# SHA256 of each release asset, from the release's SHA2-256SUMS. Verifying the download is the
# supply-chain guard — there's no advisory DB for a standalone binary, so integrity is the whole game.
# Kept in lockstep with YTDLP_VERSION by .github/workflows/ytdlp-update.yml (which refreshes both).
ARG YTDLP_SHA256_AMD64=58162f9bfdc27458ea47bfcb311cf47028f17d8154a8bf7d689861d46399230a
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) asset=yt-dlp_linux; sha=$YTDLP_SHA256_AMD64 ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/yt-dlp/yt-dlp/releases/download/${YTDLP_VERSION}/${asset}" \
      -o /usr/local/bin/yt-dlp; \
    echo "${sha}  /usr/local/bin/yt-dlp" | sha256sum -c -; \
    chmod +x /usr/local/bin/yt-dlp

# ---- runtime --------------------------------------------------------------
FROM debian:bookworm-slim

# ffmpeg (mux/faststart + cropdetect) + ca-certificates (TLS roots). curl/unzip were build-only, so
# they're gone; MP4Box comes from the build stage below, not apt.
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=tools /usr/local/bin/deno /usr/local/bin/deno
COPY --from=tools /usr/local/bin/yt-dlp /usr/local/bin/yt-dlp
# MP4Box + its shared lib (clap writer). ldconfig regenerates the libgpac.so.12 SONAME link.
COPY --from=mp4box /gpac/bin/gcc/MP4Box /usr/local/bin/MP4Box
COPY --from=mp4box /gpac/bin/gcc/libgpac.so.12.* /usr/local/lib/
RUN ldconfig
COPY --from=build /src/target/release/den-reel /usr/local/bin/den-reel

WORKDIR /app
ENV PORT=8092 \
    CACHE_DIR=/cache \
    YTDLP_PATH=/usr/local/bin/yt-dlp \
    MAX_HEIGHT=1080
VOLUME ["/cache"]
EXPOSE 8092

# The binary self-checks (no curl needed on the health path). start-period covers cold startup.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s CMD ["den-reel", "healthcheck"]
CMD ["den-reel"]
