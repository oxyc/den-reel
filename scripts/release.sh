#!/usr/bin/env bash
# Publish a release without rebuilding or retesting it.
#
#   scripts/release.sh 0.57.0
#
# Main CI has already built, behavior-probed and scanned an immutable candidate image for its exact SHA, and the
# vX.Y.Z workflow only verifies and promotes that digest. So this script's job is to make the release commit
# (the VERSION file, nothing else), wait for that commit's own main CI, and tag only once it is green.
#
# That ordering is the point. A tag on a red commit produces NO image, and `den-update` then reports "already at
# <digest>" — which means "the registry is unchanged" and reads as "nothing to deploy". The tag exists, the deploy
# command exits 0, and the box keeps serving the old binary. A tag is therefore never made before CI has passed.
#
# Do not bump Cargo.toml: the image supplies the version at run time, so a release changes no dependency layer.
set -euo pipefail

version="${1:-}"
[ -n "$version" ] || { echo "usage: scripts/release.sh <version>   e.g. 0.57.0" >&2; exit 1; }
case "$version" in
    v*) echo "error: give the version without the leading v ($version -> ${version#v})" >&2; exit 1 ;;
    *.*.*) ;;
    *) echo "error: '$version' is not a semver version" >&2; exit 1 ;;
esac
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] \
    || { echo "error: '$version' is not X.Y.Z" >&2; exit 1; }

cd "$(dirname "$0")/.."
# From the remote, so nothing here names a repo.
slug="$(git remote get-url origin | sed -e 's#.*[:/]\([^/]*/[^/]*\)$#\1#' -e 's/\.git$//')"
# Against HEAD, so a STAGED change counts too.
git diff --quiet HEAD || { echo "error: working tree has uncommitted changes" >&2; exit 1; }
git fetch --quiet origin
main="$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD | sed 's#^origin/##' || echo main)"
# These repos are worked on by several sessions at once; releasing on top of a stale main is how a push gets
# rejected halfway through.
[ "$(git rev-parse HEAD)" = "$(git rev-parse "origin/$main")" ] \
    || { echo "error: checkout is not the latest origin/$main — update it before releasing" >&2; exit 1; }
git rev-parse "v$version" >/dev/null 2>&1 && { echo "error: tag v$version already exists" >&2; exit 1; }
command -v gh >/dev/null 2>&1 || {
    echo "error: gh is required to prove exact-main CI before tagging" >&2; exit 1; }

# The checks CI runs on this exact commit, so a failure is found here rather than after the push. Formatting
# goes through nix when this machine has no rustfmt: `cargo fmt` failing with "no such command" reads as "not
# applicable", and that reading is what cost earlier releases.
run_rustfmt() {
    if cargo fmt --all --check 2>/dev/null; then return 0; fi
    command -v nix >/dev/null 2>&1 \
        || { echo "error: no rustfmt, and no nix to borrow one" >&2; return 1; }
    # shellcheck disable=SC2046
    nix run nixpkgs#rustfmt -- --edition 2021 --check $(git ls-files '*.rs')
}
echo "==> formatting";  run_rustfmt
echo "==> tests";       cargo test --quiet

current="$(cat VERSION)"
if [ "$current" != "$version" ]; then
    echo "==> version $current -> $version"
    printf '%s\n' "$version" > VERSION
    [ "$(cat VERSION)" = "$version" ] || { echo "error: the version bump did not apply" >&2; exit 1; }
    git add VERSION
    git commit --quiet -m "${slug#*/} $version"
    git push --quiet origin "HEAD:$main"
fi
sha="$(git rev-parse HEAD)"

echo "==> waiting for exact-main candidate $sha"
status=""; conclusion=""
for _ in $(seq 1 120); do
    read -r status conclusion <<<"$(gh run list --repo "$slug" --workflow ci.yml --branch "$main" --commit "$sha" \
        --event push --limit 1 --json status,conclusion --jq '.[0] | "\(.status // "-") \(.conclusion // "-")"')"
    [ "$status" = completed ] && break
    sleep 15
done
if [ "$status" != completed ]; then
    echo "error: exact-main CI did not finish within 30 minutes" >&2
    exit 1
fi
if [ "$conclusion" != success ]; then
    echo "error: exact-main CI for $sha finished $conclusion; no release tag was created" >&2
    echo "       logs: gh run list --repo $slug --workflow ci.yml" >&2
    exit 1
fi

# Do not quietly release an older generation when another session merged while CI ran. Rerunning against the new
# head needs no second version commit: VERSION already carries the intended release and its own candidate will win.
git fetch --quiet origin
[ "$sha" = "$(git rev-parse "origin/$main")" ] || {
    echo "error: origin/$main moved while CI ran — update this checkout and rerun; nothing was tagged" >&2
    exit 1
}

git tag -a "v$version" -m "${slug#*/} $version"
git push --quiet origin "v$version"

echo "==> promoting the tested candidate"
status=""; conclusion=""
for _ in $(seq 1 80); do
    # This tag's own promotion run. The newest run of ANY workflow is whatever started last, and reading that one
    # reported an image built while the build was still running. Until the tag's run exists `.[0]` is null,
    # which reads as not completed, so the loop waits for it.
    read -r status conclusion <<<"$(gh run list --repo "$slug" --workflow docker-publish.yml --branch "v$version" \
        --limit 1 --json status,conclusion --jq '.[0] | "\(.status // "-") \(.conclusion // "-")"')"
    [ "$status" = completed ] && break
    sleep 15
done
if [ "$status" != completed ]; then
    echo "note: promotion is still running after 20 minutes — gh run list --repo $slug"; exit 0
fi
[ "$conclusion" = success ] || {
    echo "error: v$version promotion failed ($conclusion); :latest was not moved" >&2
    echo "       logs: gh run view --repo $slug --log-failed" >&2
    exit 1
}
echo "v$version is signed and published. Deploy with:"
echo "  ssh root@pve 'incus exec den -- env TUF_ROOT=/var/lib/den/sigstore den-update ${slug#*/}'"
