#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 VERSION"
  echo "  VERSION  semver version, e.g. 2.7.0 or 2.7.0-rc.1"
  exit 1
}

die() { echo "ERROR: $1" >&2; exit 1; }

[ $# -eq 1 ] || usage
VERSION="$1"

# 1. Validate semver pattern
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]] || die "Invalid semver: $VERSION"

# 2. Working tree must be clean
git diff --quiet || die "Unstaged changes in working tree"
git diff --cached --quiet || die "Staged uncommitted changes"

# 3. Verify CHANGELOG.md
CHANGELOG="CHANGELOG.md"
[ -f "$CHANGELOG" ] || die "$CHANGELOG not found"

if grep -qP '^## Unreleased' "$CHANGELOG"; then
  die "CHANGELOG.md still has an '## Unreleased' section — rename it to ## [$VERSION] before releasing"
fi

if ! grep -qP "^## \[${VERSION//./\\.}\]" "$CHANGELOG"; then
  die "CHANGELOG.md has no heading for version $VERSION"
fi

# 4. Verify shard.yml
SHARD="shard.yml"
if ! grep -qP "^version: ${VERSION//./\\.}$" "$SHARD"; then
  die "shard.yml version does not match $VERSION"
fi

# 5. Verify openapi.yaml
OPENAPI="static/docs/openapi.yaml"
if ! grep -qP "version: v${VERSION//./\\.}$" "$OPENAPI"; then
  die "openapi.yaml version does not match v$VERSION"
fi

# 6. Tag must not already exist
TAG="v$VERSION"
if git rev-parse "$TAG" >/dev/null 2>&1; then
  die "Tag $TAG already exists"
fi

# 7. Extract release notes from CHANGELOG.md
ESCAPED_VERSION="${VERSION//./\\.}"
NOTES=$(sed -n "/^## \[$ESCAPED_VERSION\]/,/^## \[/{/^## \[/!p;}" "$CHANGELOG")
[ -n "$NOTES" ] || die "Could not extract release notes for $VERSION"

# 8. Let user edit tag message
TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT
printf "%s\n\n%s\n" "$TAG" "$NOTES" > "$TMPFILE"
"${EDITOR:-vi}" "$TMPFILE"

# Abort if file is empty after editing
[ -s "$TMPFILE" ] || die "Tag message is empty, aborting"

# 9. Create annotated tag
git tag -a "$TAG" -F "$TMPFILE"
echo "Created tag $TAG"

# 10. Push tag (with confirmation)
read -rp "Push $TAG to origin? [y/N] " REPLY
if [[ "$REPLY" =~ ^[Yy]$ ]]; then
  git push origin "$TAG"
  echo "Pushed $TAG to origin"
else
  echo "Tag $TAG created locally but not pushed"
fi
