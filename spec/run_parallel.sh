#!/bin/sh
# Builds the spec suite once and runs it as SPEC_JOBS processes (default: one
# per CPU), each running every SPEC_JOBS-th example via the spec runner's
# SPEC_SPLIT. Examples are shuffled before they're split, so all processes
# share one seed (SPEC_SEED, default random). Arguments are passed to every
# process, e.g. --tag. A run is reproduced by its SPEC_JOBS and SPEC_SEED.
#
# Output is streamed live with a "[n] " process prefix, then the failures of
# all processes and one summary in the spec runner's format are printed.
set -eu

jobs=${SPEC_JOBS:-$(getconf _NPROCESSORS_ONLN)}
seed=${SPEC_SEED:-$(od -An -N2 -tu2 /dev/urandom | tr -d ' ')}
pids=""
dir=$(mktemp -d)
trap 'kill $pids 2>/dev/null || true; rm -rf "$dir"' EXIT
trap 'exit 130' INT TERM

echo 'require "./**"' | crystal build --stdin-filename spec/all_specs.cr -o "$dir/specs"

echo "Randomized with seed: $seed, in $jobs processes"
start=$(date +%s)
i=0
while [ "$i" -lt "$jobs" ]; do
  {
    status=0
    SPEC_SPLIT="$i%$jobs" "$dir/specs" --order "$seed" "$@" 2>&1 || status=$?
    echo "$status" > "$dir/$i.status"
  } | tee "$dir/$i.log" | awk -v prefix="[$i] " '{ print prefix $0; fflush() }' &
  pids="$pids $!"
  i=$((i + 1))
done
for pid in $pids; do wait "$pid" || true; done
pids=""
elapsed=$(($(date +%s) - start))

failed=0
i=0
while [ "$i" -lt "$jobs" ]; do
  if [ "$(cat "$dir/$i.status" 2>/dev/null)" != 0 ]; then
    failed=$((failed + 1))
    echo
    echo "Process $i of $jobs failed:"
    # Its report from the first failure on, or all of it if it crashed
    awk '/^Failures:/ { found = 1 } found { print } END { exit !found }' "$dir/$i.log" || cat "$dir/$i.log"
  fi
  i=$((i + 1))
done

echo
echo "Finished in $elapsed seconds"
cat "$dir"/*.log | awk '
  / examples?, [0-9]+ failures?, [0-9]+ errors?, [0-9]+ pending$/ { e += $1; f += $3; x += $5; p += $7 }
  END { printf "%d examples, %d failures, %d errors, %d pending\n", e, f, x, p }'
echo "Randomized with seed: $seed, in $jobs processes"
if [ "$failed" -ne 0 ]; then
  echo "$failed of $jobs processes failed, reproduce with: SPEC_JOBS=$jobs SPEC_SEED=$seed make test"
  exit 1
fi
