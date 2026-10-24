#!/usr/bin/env bash
# Cut a release: bump the root VERSION, tag it v<version>, push the release branch
# and tag. The tag push is what the release pipeline (.github/workflows/release.yml)
# reacts to — it builds, tests, verifies and publishes each language's package.
#
#   ./release.sh 2.1.0      # -> branch release/2.1.x, tag v2.1.0
#
# This bumps the root VERSION *and* each language's package manifest (so the committed
# sources always carry the release version); CI re-verifies they match the tag before
# building and publishing.
set -euo pipefail
cd "$(dirname "$0")"

die() { echo "error: $*" >&2; exit 1; }
confirm() { read -r -p "$1 [Y/n] " a; [[ -z "$a" || "$a" =~ ^[Yy]$ ]]; }

VERSION="${1:-}"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "usage: $0 X.Y.Z   (got: '${VERSION:-<none>}')"

IFS='.' read -r MAJOR MINOR _PATCH <<< "$VERSION"
BRANCH="release/${MAJOR}.${MINOR}.x"
TAG="v${VERSION}"

# Per-language manifests that carry the version (keep in sync with CI's verify steps).
PYPROJECT="python/pyproject.toml"
CSPROJ="csharp/src/ConnPool/ConnPool.csproj"
POM="java/pom.xml"

# 1. clean working tree (nothing modified, staged, or untracked).
[[ -z "$(git status --porcelain)" ]] || die "working tree not clean — commit or stash first."

# 2. tag must not already exist.
git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null && die "tag ${TAG} already exists."

# 3. make sure we are on the right release branch, offer to create/switch otherwise.
current="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current" != "$BRANCH" ]]; then
    echo "on '${current}', release of ${VERSION} belongs on '${BRANCH}'."
    confirm "switch to ${BRANCH} (creating it if needed)?" || die "aborted."
    if git show-ref --verify --quiet "refs/heads/${BRANCH}"; then
        git checkout "$BRANCH"
    else
        git checkout -b "$BRANCH"
    fi
fi

# 4. bump the version in the root file and every language manifest.
echo "$VERSION" > VERSION
sed -i -E "s/^version = \".*\"/version = \"${VERSION}\"/" "$PYPROJECT"
sed -i -E "s|<Version>[^<]*</Version>|<Version>${VERSION}</Version>|" "$CSPROJ"
sed -i -E "0,/<version>[^<]*<\/version>/s||<version>${VERSION}</version>|" "$POM"

# Guard against a manifest whose line didn't match the expected pattern (silent no-op).
grep -q "^version = \"${VERSION}\"" "$PYPROJECT" || die "failed to set version in ${PYPROJECT}"
grep -q "<Version>${VERSION}</Version>" "$CSPROJ" || die "failed to set version in ${CSPROJ}"
grep -q "<version>${VERSION}</version>" "$POM" || die "failed to set version in ${POM}"

# 5. show what's about to happen and confirm before anything leaves this machine.
git add VERSION "$PYPROJECT" "$CSPROJ" "$POM"
git status
echo
echo "release:  ${VERSION}"
echo "branch:   ${BRANCH}"
echo "tag:      ${TAG}"
echo "remote:   $(git remote get-url origin 2>/dev/null || echo '<no origin>')"
echo
confirm "commit, tag and push the above?" || die "aborted — VERSION staged but nothing pushed."

# 6. commit (only if the version changed), tag, push branch + tag.
if git diff --cached --quiet; then
    echo "VERSION already ${VERSION}; tagging the current commit without a new bump."
else
    git commit -m "release: ${TAG}"
fi
git tag -a "$TAG" -m "release ${TAG}"    # -a -m => annotated with a message, never opens an editor
git push origin "$BRANCH"
git push origin "$TAG"

echo "pushed ${BRANCH} and ${TAG} — release pipeline will take it from here."
