#!/bin/sh
# Builds bdwgc with mark-stack-overflow.patch and installs libgc.a where
# crystal links it, replacing the one from the 84codes/crystal image.
# Same version and configure flags as the 84codes/crystal images and the CFLAGS
# each image gives its own libgc.a, so the patch is the only intended difference.
# Debian compiles with the default -g -O2 and then strips, so -O2 matches it.
# Usage: build.sh [destdir]   (default: crystal env CRYSTAL_LIBRARY_PATH)
set -eu
version=8.2.12
sha256=42e5194ad06ab6ffb806c83eb99c03462b495d979cda782f3c72c08af833cd4e
src=$(cd "$(dirname "$0")" && pwd)
dest=${1:-$(crystal env CRYSTAL_LIBRARY_PATH)}
# shellcheck disable=SC1091
case "$(. /etc/os-release 2>/dev/null && echo "$ID ${ID_LIKE:-}")" in
  alpine*) export CFLAGS="-Os -fstack-clash-protection -Wformat -Werror=format-security -fno-plt" ;;
  *fedora*|*rhel*) export CFLAGS="-DNO_GETCONTEXT -pipe -fPIE -O3" ;;
  *debian*) export CFLAGS="-O2" ;;
esac
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"
curl -fsSLo gc.tar.gz "https://github.com/ivmai/bdwgc/releases/download/v$version/gc-$version.tar.gz"
echo "$sha256  gc.tar.gz" | sha256sum -c - >/dev/null
tar xzf gc.tar.gz
cd "gc-$version"
patch -s -p1 < "$src/mark-stack-overflow.patch"
./configure -q --disable-debug --disable-shared --enable-large-config >/dev/null
echo "CFLAGS: $(sed -n 's/^CFLAGS = //p' Makefile)"
make -s -j"$(nproc)" >/dev/null
cc -O2 -pthread -DHAVE_CONFIG_H -I. -Iinclude "$src/mark_stack_test.c" .libs/libgc.a -o mark_stack_test
./mark_stack_test
install -D -m 644 .libs/libgc.a "$dest/libgc.a"
echo "installed $dest/libgc.a"
