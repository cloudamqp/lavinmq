#!/usr/bin/env bash
#
# Set up a shovel-per-HTTP-status test against a local LavinMQ.
#
# For each status code it:
#   1. creates a durable queue  shovel-test-<code>
#   2. binds it to amq.topic with routing key "#"
#   3. creates a shovel that consumes the queue and POSTs to
#      http://<endpoint>/<code>   (the shovel_test_endpoint.cr server)
#
# Because every queue is bound with "#", a single publish to amq.topic fans out
# to all of them, so one message exercises every disposition at once:
#
#   200 -> Confirmed   503/408/429 -> Retry   400/422 -> Reject   others -> Abort
#
# Usage:
#   ./shovel_status_test.sh setup            # create queues, bindings, shovels
#   ./shovel_status_test.sh publish [rk] [n] # publish n messages (default 1) to amq.topic
#   ./shovel_status_test.sh list             # show the shovels and their state
#   ./shovel_status_test.sh cleanup          # delete the shovels and queues
#
# Config via env:
#   API       management base URL        (default http://localhost:15672)
#   USERPASS  management credentials      (default guest:guest)
#   VHOST     virtual host                (default /)
#   AMQP_URI  shovel source uri           (default amqp://guest:guest@localhost)
#   ENDPOINT  test web server host:port   (default localhost:8888)
#   CODES     space separated status list (default below)

set -euo pipefail

API="${API:-http://localhost:15672}"
USERPASS="${USERPASS:-guest:guest}"
VHOST="${VHOST:-/}"
AMQP_URI="${AMQP_URI:-amqp://guest:guest@localhost}"
ENDPOINT="${ENDPOINT:-localhost:8888}"
CODES="${CODES:-200 400 422 408 429 503 401 403 404 410 418}"
EXCHANGE="amq.topic"
PREFIX="shovel-test"

# URL-encode the vhost ("/" -> "%2f")
vh=$(printf '%s' "$VHOST" | sed 's#/#%2f#g')

api() { # method path [json]
  local method="$1" path="$2" data="${3:-}"
  if [ -n "$data" ]; then
    curl -fsS -u "$USERPASS" -X "$method" \
      -H 'content-type: application/json' \
      -d "$data" "$API$path"
  else
    curl -fsS -u "$USERPASS" -X "$method" "$API$path"
  fi
}

setup() {
  for code in $CODES; do
    local q="$PREFIX-$code"
    echo "==> $q  ->  http://$ENDPOINT/$code"

    # 1. queue
    api PUT "/api/queues/$vh/$q" '{"durable":true,"auto_delete":false}' >/dev/null

    # 2. binding to amq.topic with routing key "#"
    api POST "/api/bindings/$vh/e/$EXCHANGE/q/$q" '{"routing_key":"#"}' >/dev/null

    # 3. shovel: consume the queue, POST to the endpoint path for this code.
    #    dest-exchange:"" is required only to satisfy config validation, which
    #    demands a dest queue/exchange even for HTTP destinations; the HTTP
    #    destination ignores it.
    api PUT "/api/parameters/shovel/$vh/$q" "$(cat <<JSON
{"value":{
  "src-uri": "$AMQP_URI",
  "src-queue": "$q",
  "dest-uri": "http://$ENDPOINT/$code",
  "dest-exchange": "",
  "ack-mode": "on-confirm",
  "src-delete-after": "never"
}}
JSON
)" >/dev/null
  done
  echo "done. publish with: $0 publish"
}

publish() {
  local rk="${1:-test}" n="${2:-1}" i
  for ((i = 1; i <= n; i++)); do
    api POST "/api/exchanges/$vh/$EXCHANGE/publish" "$(cat <<JSON
{"properties":{"content_type":"text/plain","message_id":"msg-$i"},
 "routing_key":"$rk","payload":"hello $i","payload_encoding":"string"}
JSON
)" >/dev/null
  done
  echo "published $n message(s) to $EXCHANGE (rk=$rk); fans out to all $PREFIX-* queues"
}

list() {
  api GET "/api/shovels/$vh" | tr ',' '\n' | grep -E '"name"|"state"|"error"' || true
}

cleanup() {
  for code in $CODES; do
    local q="$PREFIX-$code"
    api DELETE "/api/parameters/shovel/$vh/$q" >/dev/null 2>&1 || true
    api DELETE "/api/queues/$vh/$q" >/dev/null 2>&1 || true
    echo "removed $q"
  done
}

case "${1:-setup}" in
  setup) setup ;;
  publish) shift; publish "$@" ;;
  list) list ;;
  cleanup) cleanup ;;
  *) echo "usage: $0 {setup|publish [rk] [n]|list|cleanup}" >&2; exit 2 ;;
esac
