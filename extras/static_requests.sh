#!/bin/bash

set -e

PORT=${PORT:-15672}
FAILURES=0

request() {
  local path=${1}
  local body_size=$(curl --silent --output /dev/null --write-out \
  "%{size_download}\n" "http://127.0.0.1:${PORT}/${path}")

  [[ $body_size -eq 0 ]] && echo "FAIL: $path" && ((FAILURES += 1)) || :
}

# test all static files, except empty ones like .gitkeep
for path in $(find static -type f ! -path '*/.*' | sed 's/static\///')
do
  # Ignore any path with a dir or file starting with _
  [[ $path =~ (^_|/_) ]] && continue
  request $path
done

[[ $FAILURES -gt 0 ]] && exit 1 || true
