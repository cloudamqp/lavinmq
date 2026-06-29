#!/usr/bin/env bash
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SESSION="lavinmq-tui-inspect-$$"
TMPDIR_PATH=$(mktemp -d "${TMPDIR:-/tmp}/lavinmq-tui.XXXXXX")
OUT_DIR=""
URI=""
MOCK_PORT="15692"
INTERVAL="60"
KEEP_SESSION="0"

usage() {
  cat <<'USAGE'
Usage: extras/tui_inspect.sh [options]

Run lavinmqctl tui in tmux, capture each page, and quit.

Options:
  --uri=URI             Management API URI. Starts the mock API when omitted.
  --output=DIR          Output directory for captured pages.
  --mock-port=PORT      Port for the mock API (default 15692).
  --interval=SECONDS    TUI poll interval (default 60).
  --keep-session        Leave the tmux session running.
  -h, --help            Show this help.
USAGE
}

for arg in "$@"; do
  case "$arg" in
    --uri=*) URI=${arg#*=} ;;
    --output=*) OUT_DIR=${arg#*=} ;;
    --mock-port=*) MOCK_PORT=${arg#*=} ;;
    --interval=*) INTERVAL=${arg#*=} ;;
    --keep-session) KEEP_SESSION="1" ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $arg" >&2; usage >&2; exit 1 ;;
  esac
done

if [ -z "$OUT_DIR" ]; then
  OUT_DIR="$TMPDIR_PATH/captures"
fi
mkdir -p "$OUT_DIR"

MOCK_PID=""
cleanup() {
  if [ "$KEEP_SESSION" != "1" ]; then
    tmux kill-session -t "$SESSION" 2>/dev/null || true
  fi
  if [ -n "$MOCK_PID" ]; then
    kill "$MOCK_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

if [ -z "$URI" ]; then
  URI="http://127.0.0.1:$MOCK_PORT"
  crystal run "$ROOT/extras/tui_mock_api.cr" -- --port "$MOCK_PORT" >"$TMPDIR_PATH/mock.log" 2>&1 &
  MOCK_PID=$!
  i=0
  while ! curl -fsS "$URI/api/overview" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -gt 100 ]; then
      echo "mock API did not start; see $TMPDIR_PATH/mock.log" >&2
      exit 1
    fi
    sleep 0.1
  done
fi

RUNNER="$TMPDIR_PATH/run-tui.sh"
cat >"$RUNNER" <<EOF
#!/usr/bin/env bash
set -eu
cd "$ROOT"
exec crystal run src/lavinmqctl.cr -- --uri "$URI" tui -i "$INTERVAL"
EOF
chmod +x "$RUNNER"

tmux new-session -d -s "$SESSION" -x 140 -y 36 "$RUNNER"

wait_for_text() {
  text=$1
  i=0
  while ! tmux capture-pane -p -t "$SESSION" | grep -F "$text" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -gt 200 ]; then
      echo "timed out waiting for TUI text: $text" >&2
      tmux capture-pane -p -t "$SESSION" >&2 || true
      exit 1
    fi
    sleep 0.1
  done
}

wait_for_text "LavinMQ TUI"

capture_page() {
  key=$1
  name=$2
  title=$3
  if [ "$key" != "-" ]; then
    tmux send-keys -t "$SESSION" "$key"
  fi
  wait_for_text "| $title |"
  tmux capture-pane -p -t "$SESSION" > "$OUT_DIR/$name.txt"
}

capture_page "-" "01-overview" "Overview"
capture_page "2" "02-queues" "Queues"
capture_page "3" "03-connections" "Connections"
capture_page "4" "04-channels" "Channels"
capture_page "5" "05-exchanges" "Exchanges"
capture_page "6" "06-consumers" "Consumers"
capture_page "7" "07-vhosts" "Vhosts"
capture_page "8" "08-nodes" "Nodes"
capture_page "9" "09-parameters" "Parameters"
capture_page "0" "10-policies" "Policies"
capture_page "s" "11-shovels" "Shovels"
capture_page "f" "12-federation" "Federation"
capture_page "u" "13-users" "Users"

tmux send-keys -t "$SESSION" "q"

echo "at=info event=tui_captured output_dir=$OUT_DIR uri=$URI"
