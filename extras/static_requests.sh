#!/bin/sh

PORT=${PORT:-15672}
FAILURES=0

request() {
  local path=$1
  local body_size=$(curl --silent --output /dev/null --write-out "%{size_download}\n" "http://127.0.0.1:${PORT}${path}")

  [[ $body_size -eq 0 ]] && echo "FAIL: $path" && ((FAILURES += 1))
}

request /
request /robots.txt
request /img/favicon.png
request /test/
request /test
request /test/index.html

[[ $FAILURES -gt 0 ]] && exit 1 || true
