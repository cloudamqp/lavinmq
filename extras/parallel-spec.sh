#!/bin/sh
# Builds the spec suite once and runs it as SPEC_JOBS processes (default: one
# per CPU), each running every SPEC_JOBS-th example via the spec runner's
# SPEC_SPLIT. Examples are shuffled before they're split, so all processes
# share one seed (SPEC_SEED, default random). Arguments are passed to every
# process, e.g. --tag. A run is reproduced by its SPEC_JOBS and SPEC_SEED.
set -eu

jobs=${SPEC_JOBS:-$(getconf _NPROCESSORS_ONLN)}
seed=${SPEC_SEED:-$(od -An -N2 -tu2 /dev/urandom | tr -d ' ')}
pids=""
dir=$(mktemp -d)
trap 'kill $pids 2>/dev/null || true; rm -rf "$dir"' EXIT
trap 'exit 130' INT TERM

echo 'require "./**"' | crystal build --stdin-filename spec/all_specs.cr -o "$dir/specs"

i=0
while [ "$i" -lt "$jobs" ]; do
  SPEC_SPLIT="$i%$jobs" "$dir/specs" --order "$seed" "$@" > "$dir/$i.log" 2>&1 &
  pids="$pids $!"
  i=$((i + 1))
done

status=0
i=0
for pid in $pids; do
  if wait "$pid"; then
    echo "=== Spec process $i of $jobs ==="
  else
    status=1
    echo "=== Spec process $i of $jobs FAILED ==="
  fi
  cat "$dir/$i.log"
  i=$((i + 1))
done
pids=""
echo "Ran in $jobs processes, reproduce with: SPEC_JOBS=$jobs SPEC_SEED=$seed make test"
exit $status
