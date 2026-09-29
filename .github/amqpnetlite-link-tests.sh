#!/usr/bin/env bash
# Runs AMQP.Net Lite's link tests against a running LavinMQ.
#
# The suite normally runs against its own in-process broker, whose
# auto-created queue disappears when a test's connection closes, so tests
# leave released and modified messages behind. Here all tests share one
# pre-declared queue, so each test runs on its own with the queue purged
# first.
#
# Usage: amqpnetlite-link-tests.sh path/to/Test.Amqp.Net.dll
set -euo pipefail

dll=$1
http=${LAVINMQ_HTTP_URL:-http://127.0.0.1:15672}
queue=q1
# The suite strips the leading / of the URL path, hence the double slash
export AMQPNETLITE_TESTTARGET="amqp://guest:guest@127.0.0.1:5672//queues/$queue"
# JMS selector filters (apache.org:selector-filter:string) are not supported
skip=" TestMethod_ReceiveWithFilter "

for _ in $(seq 60); do
  curl -fsS -o /dev/null -u guest:guest "$http/api/overview" && break
  sleep 0.5
done
curl -fsS -u guest:guest -X PUT -H 'content-type: application/json' -d '{"durable":false}' \
  "$http/api/queues/%2F/$queue"

mapfile -t tests < <(dotnet test "$dll" --list-tests --filter "FullyQualifiedName~Test.Amqp.LinkTests" |
  sed -n 's/^    \(TestMethod_[A-Za-z0-9_]*\)$/\1/p')
if [ ${#tests[@]} -eq 0 ]; then
  echo "No link tests found" >&2
  exit 1
fi

failed=()
for test in "${tests[@]}"; do
  if [[ $skip == *" $test "* ]]; then
    echo "SKIP $test"
    continue
  fi
  curl -fsS -u guest:guest -X DELETE "$http/api/queues/%2F/$queue/contents"
  if output=$(timeout 300 dotnet test "$dll" --filter "FullyQualifiedName=Test.Amqp.LinkTests.$test" 2>&1); then
    echo "PASS $test"
  else
    echo "FAIL $test"
    echo "$output"
    failed+=("$test")
  fi
done

echo "${#tests[@]} tests, ${#failed[@]} failed"
if [ ${#failed[@]} -gt 0 ]; then
  printf 'Failed: %s\n' "${failed[@]}"
  exit 1
fi
