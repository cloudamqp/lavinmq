#!/usr/bin/env bash
# Model checks the TLA+ specs with TLC, see README.md.
#
#   extras/tla/check.sh [quick|full] [model...]
#
# Each MC_*.cfg says in its header what TLC must find and how to search:
#   \* Expect: pass            every invariant holds
#   \* Expect: <Invariant>     TLC must find a counterexample to <Invariant>
#   \* Mode: bfs               exhaustive (breadth first, shortest counterexample)
#   \* Mode: simulate          random behaviours for TLA_SIM_SECONDS
#   \* Tier: quick|full        quick models run in CI, full ones too with `full`
set -euo pipefail

cd "$(dirname "$0")"

TLA_VERSION=1.7.4
TLA_SHA256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
JAR=.cache/tla2tools-$TLA_VERSION.jar
WORKERS=${TLA_WORKERS:-auto}
SIM_SECONDS=${TLA_SIM_SECONDS:-120}

tier=${1:-quick}
shift || true

if [ ! -f "$JAR" ]; then
  mkdir -p .cache
  curl -sSfL -o "$JAR.tmp" \
    "https://github.com/tlaplus/tlaplus/releases/download/v$TLA_VERSION/tla2tools.jar"
  echo "$TLA_SHA256  $JAR.tmp" | sha256sum -c --quiet
  mv "$JAR.tmp" "$JAR"
fi

header() { sed -n "s/^\\\\\\* $2: *//p" "$1" | head -1; }

if [ $# -gt 0 ]; then
  models=("$@")
else
  models=()
  for cfg in MC_*.cfg; do
    model=${cfg%.cfg}
    model_tier=$(header "$cfg" Tier)
    if [ "$tier" = full ] || [ "$model_tier" = quick ]; then
      models+=("$model")
    fi
  done
fi

failed=()
for model in "${models[@]}"; do
  cfg=$model.cfg
  expect=$(header "$cfg" Expect)
  mode=$(header "$cfg" Mode)
  out=.cache/$model.out
  args=(-workers "$WORKERS" -metadir ".cache/states/$model" -config "$cfg" -deadlock)
  if [ "$mode" = simulate ]; then
    args+=(-simulate -depth 100)
    jvm=(-Dtlc2.TLC.stopAfter="$SIM_SECONDS")
  else
    jvm=()
  fi
  echo "== $model ($mode, expect $expect)"
  rm -rf ".cache/states/$model"
  start=$SECONDS
  java -XX:+UseParallelGC "${jvm[@]}" -cp "$JAR" tlc2.TLC "${args[@]}" MC > "$out" 2>&1 || true
  took=$((SECONDS - start))
  violated=$(sed -n 's/^Error: Invariant \(.*\) is violated.*/\1/p' "$out" | head -1)
  stats=$(grep -E '^[0-9,]+ states generated' "$out" | tail -1 || true)
  if grep -qE '^Error: (Parsing|Semantic|TLC threw|The exception)|^Exception' "$out"; then
    result="error, see $out"
  elif [ "$expect" = pass ] && [ -z "$violated" ]; then
    result=ok
  elif [ "$expect" != pass ] && [ "$violated" = "$expect" ]; then
    result="ok (found the expected counterexample)"
  elif [ -n "$violated" ]; then
    result="FAILED: $violated violated, see $out"
  else
    result="FAILED: expected a counterexample to $expect, see $out"
  fi
  echo "   $result in ${took}s; $stats"
  [[ $result == ok* ]] || failed+=("$model")
done

if [ ${#failed[@]} -gt 0 ]; then
  echo "Failed: ${failed[*]}"
  exit 1
fi
