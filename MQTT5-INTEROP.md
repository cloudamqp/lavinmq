# MQTT interop harness - how to re-run the external verification

Companion to `MQTT5.md`. `MQTT5-TESTING.md` records *what* the 2026-08-19
external run found; this file is *how to run it again*. Nothing here is wired into `make`
or CI on purpose: it needs a built binary, a network clone and a Docker pull, and
it is a release-gate check, not a per-commit one.

Last run 2026-10-02, on `9e3d3559` (QoS 2 included); the score and what each
failure maps to are under *Interpreting the score*. Re-run it after E / F land:
several of the remaining Paho failures are the grading function for exactly
those items.

## What it exercises that our own specs cannot

`spec/mqtt` drives the broker through `MQTT::Protocol::IO.v5` - the same codec
the broker encodes with - so a self-consistent wire-format mistake is invisible to
it. These tools bring their own codecs:

| tool | what it is good for |
|---|---|
| Eclipse Paho interoperability suite | 27 v5 + 9 v3.1.1 broker conformance tests, written against the spec by the people who wrote the reference client |
| paho-mqtt (Python) | property round-trips, capability inspection, wills |
| mqtt.js (Node) | a third independent codec |
| mosquitto clients | `-D` sets any v5 property by hand, so one command per row of the compliance table in `MQTT5.md`; prints the DISCONNECT reason code it receives |
| hand-built raw packets | the byte-exact cases no library will let you send (packet id 0, a second CONNECT, a 5-byte Maximum Packet Size) |

## Prerequisites

`python3`, `node`/`npm`, `docker`, network access. No `sudo`: the mosquitto
clients come from a container, and `paho-mqtt` goes in a venv (Debian's Python is
PEP 668 externally-managed).

## Setup

Everything lives in a scratch directory - nothing is written into the repo.

```sh
export LMQ=/path/to/lavinmq-worktree
export W=/tmp/mqtt-interop            # anything outside the repo
export MQTT_PORT=1893                 # broker.sh, run_suite.sh and the drivers all read it
mkdir -p "$W/results" && cd "$W"

# 0. the binary under test
( cd "$LMQ" && make bin/lavinmq CRYSTAL_FLAGS= )
"$LMQ/bin/lavinmq" --version

# 1. the conformance suite (stdlib only - the client it drives is vendored)
git clone --depth 1 https://github.com/eclipse-paho/paho.mqtt.testing.git

# 2. LavinMQ requires credentials on every CONNECT, and the suite sends none.
#    Patch the two vendored clients' connect() defaults instead of ~27 call sites.
sed -i 's/willRetain=False, username=None, password=None/willRetain=False, username="guest", password=b"guest"/' \
  paho.mqtt.testing/interoperability/mqtt/clients/V5/main.py \
  paho.mqtt.testing/interoperability/mqtt/clients/V311/main.py

# client_test.py forgets to strip -p from argv, so unittest chokes on it
python3 - <<'EOF'
p = "paho.mqtt.testing/interoperability/client_test.py"
s = open(p).read()
old = '''    elif o in ("-p", "--port"):
      port = int(a)'''
s = s.replace(old, old + '''
      sys.argv.remove("-p") if "-p" in sys.argv else sys.argv.remove("--port")
      sys.argv.remove(a)''', 1)
open(p, "w").write(s)
EOF

# 3. the real client libraries
python3 -m venv venv && ./venv/bin/pip -q install 'paho-mqtt>=2,<3'
mkdir -p node && ( cd node && npm init -y >/dev/null && npm install --silent mqtt )
docker pull -q eclipse-mosquitto
export NODE_PATH="$W/node/node_modules"
```

## The scripts

Write these five files into `$W`.

<details>
<summary><code>broker.sh</code> - start/stop a broker on a fresh data dir</summary>

```bash
#!/usr/bin/env bash
# usage: broker.sh start <logfile> | broker.sh stop
set -u
W="$(cd "$(dirname "$0")" && pwd)"
BIN="${LMQ:?set LMQ to the lavinmq worktree}/bin/lavinmq"
DATA="$W/data"
PIDFILE="$W/broker.pid"
# Off the default ports, so another LavinMQ on the machine cannot answer for us.
MQTT_PORT="${MQTT_PORT:-1893}"

case "${1:-}" in
start)
  LOG="${2:-$W/broker.log}"
  "$0" stop
  rm -rf "$DATA"; mkdir -p "$DATA"
  "$BIN" --data-dir "$DATA" --bind 127.0.0.1 \
         --mqtt-port "$MQTT_PORT" --amqp-port 5683 --http-port 15683 \
         --mqtts-port -1 --amqps-port -1 --metrics-http-port 15693 \
         --control-unix-path /tmp/lmqi.ctl.sock \
         --debug > "$LOG" 2>&1 &
  echo $! > "$PIDFILE"
  for _ in $(seq 1 100); do
    # Our own process must still be alive: a port that answers may be someone else's.
    kill -0 "$(cat "$PIDFILE")" 2>/dev/null || break
    if (exec 3<>/dev/tcp/127.0.0.1/"$MQTT_PORT") 2>/dev/null; then
      echo "broker up (pid $(cat "$PIDFILE")), log $LOG"; exit 0
    fi
    sleep 0.1
  done
  echo "broker failed to start"; tail -20 "$LOG"; exit 1
  ;;
stop)
  if [ -f "$PIDFILE" ]; then
    kill "$(cat "$PIDFILE")" 2>/dev/null
    for _ in $(seq 1 50); do kill -0 "$(cat "$PIDFILE")" 2>/dev/null || break; sleep 0.1; done
    rm -f "$PIDFILE"
  fi
  ;;
*) echo "usage: $0 start [logfile] | $0 stop"; exit 2 ;;
esac
```

`$LMQ` is the worktree exported during setup. Every port is off LavinMQ's
defaults (MQTT 1893, AMQP 5683, HTTP 15683, metrics 15693, and its own
`lavinmqctl` socket), so a LavinMQ already running on the machine cannot answer
for this one. That happened on 2026-10-02: a dev broker held 1883, ours failed to
bind, and the old readiness check - "does the port answer?" - reported it up, so
a whole run graded the wrong server. The check now also requires our own process
to be alive. A **fresh data dir per run** is not optional: retained messages and
persisted sessions leak between suites otherwise.

To run a second broker in parallel (useful: one suite on 1893 while you poke at
1894), copy the script and change `DATA`, `PIDFILE`, all three ports, **and** give
it its own `--metrics-http-port` and `--control-unix-path`. Two instances
otherwise fight over the same socket and the second dies. Keep those socket paths short - a long path trips the 107-byte
`sockaddr_un` limit.

</details>

<details>
<summary><code>run_suite.sh</code> - the Paho suite, one test at a time</summary>

```bash
#!/usr/bin/env bash
# usage: run_suite.sh <suite-dir> <client_test5.py|client_test.py> <out-dir>
set -u
W="$(cd "$(dirname "$0")" && pwd)"
IOP="$W/$1/interoperability"; SUITE="$2"
OUT="$(mkdir -p "$3" && cd "$3" && pwd)"   # absolute: we cd into the suite below
TESTS=$(grep -o "  def test_[a-z0-9_]*" "$IOP/$SUITE" | sed 's/.*def //')
for t in $TESTS; do
  "$W/broker.sh" start "$OUT/$t.broker.log" >/dev/null || { echo "$t BROKER-FAIL"; continue; }
  cd "$IOP"
  timeout 180 python3 -u "$SUITE" -p "${MQTT_PORT:-1893}" "Test.$t" > "$OUT/$t.out" 2>&1
  rc=$?
  "$W/broker.sh" stop
  if [ $rc -eq 0 ]; then st=PASS
  elif [ $rc -eq 124 ]; then st=TIMEOUT
  else st=FAIL; fi
  crash=""
  grep -qiE "unhandled exception|Invalid memory access|BUG:" "$OUT/$t.broker.log" && crash=" BROKER-CRASH"
  printf "%-32s %s%s\n" "$t" "$st" "$crash" | tee -a "$OUT/summary.txt"
done
```

One broker per test, so a test that wedges a session cannot contaminate the next
one, and a hang costs 180s instead of the whole run. Usage:

```sh
./run_suite.sh paho.mqtt.testing client_test5.py results/v5
./run_suite.sh paho.mqtt.testing client_test.py  results/v3
```

Each test leaves `results/<dir>/<test>.out` (client side) next to
`<test>.broker.log` (server side, `--debug`). Read them as a pair: "client hung"
plus `WARN Protocol violation` is a correct rejection, not a bug. (Before QoS 2
that line read `QoSNotSupported`; it should no longer appear.)

</details>

<details>
<summary><code>interop.py</code> - paho-mqtt driver (capabilities, pub, sub)</summary>

```python
#!/usr/bin/env python3
"""paho-mqtt driver for LavinMQ MQTT interop checks.

  interop.py caps                                   # dump CONNACK capabilities
  interop.py sub <topic> [count] [timeout] [5|311] [qos] [nl,rap,rh=N]
  interop.py pub <topic> <payload> [qos] [retain|-] [5|311]

Env: MQTT_PORT (default 1893). Credentials are always guest/guest.
Subscriber prints one JSON line per event, so callers can grep for "props".
"""
import json, os, sys, threading, time
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.subscribeoptions import SubscribeOptions

PORT = int(os.environ.get("MQTT_PORT", "1893"))
PUB_PROPS = ("PayloadFormatIndicator", "MessageExpiryInterval", "ContentType",
             "ResponseTopic", "CorrelationData", "UserProperty",
             "SubscriptionIdentifier", "TopicAlias")
CONNACK_PROPS = ("MaximumQoS", "RetainAvailable", "WildcardSubscriptionAvailable",
                 "SubscriptionIdentifierAvailable", "SharedSubscriptionAvailable",
                 "TopicAliasMaximum", "MaximumPacketSize", "ReceiveMaximum",
                 "ServerKeepAlive", "SessionExpiryInterval",
                 "AssignedClientIdentifier", "ResponseInformation", "ReasonString")

def client(cid, ver):
    proto = MQTTProtocolVersion.MQTTv5 if ver == "5" else MQTTProtocolVersion.MQTTv311
    c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=cid, protocol=proto)
    c.username_pw_set("guest", "guest")
    return c, proto

def dump(props, names):
    out = {}
    for n in names:
        if props is not None and hasattr(props, n):
            v = getattr(props, n)
            out[n] = v.decode("utf-8", "replace") if isinstance(v, bytes) else v
    return out

cmd = sys.argv[1]

if cmd == "caps":
    c, _ = client("interop-caps", "5")
    def on_connect(cl, u, flags, rc, props):
        print(json.dumps({"reason_code": str(rc), "session_present": flags.session_present,
                          "props": dump(props, CONNACK_PROPS)}, indent=2))
        cl.disconnect()
    c.on_connect = on_connect
    c.connect("127.0.0.1", PORT, 30)
    c.loop_forever()

elif cmd == "sub":
    topic = sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    timeout = float(sys.argv[4]) if len(sys.argv) > 4 else 10
    ver = sys.argv[5] if len(sys.argv) > 5 else "5"
    qos = int(sys.argv[6]) if len(sys.argv) > 6 else 1
    opts = sys.argv[7] if len(sys.argv) > 7 else ""
    got, done = [], threading.Event()
    c, proto = client("interop-sub", ver)
    def on_connect(cl, u, flags, rc, props=None):
        print(json.dumps({"event": "connack", "rc": str(rc)}), flush=True)
        if proto == MQTTProtocolVersion.MQTTv5:
            cl.subscribe(topic, options=SubscribeOptions(
                qos=qos, noLocal="nl" in opts, retainAsPublished="rap" in opts,
                retainHandling=int(opts.split("rh=")[1][0]) if "rh=" in opts else 0))
        else:
            cl.subscribe(topic, qos)
    def on_subscribe(cl, u, mid, rcs, props=None):
        print(json.dumps({"event": "suback", "codes": [str(r) for r in rcs]}), flush=True)
    def on_disconnect(cl, u, flags, rc, props=None):
        print(json.dumps({"event": "disconnect", "rc": str(rc)}), flush=True)
        done.set()
    def on_message(cl, u, msg):
        got.append(1)
        print(json.dumps({"event": "message", "topic": msg.topic, "qos": msg.qos,
                          "retain": msg.retain, "payload": msg.payload.decode("utf-8", "replace"),
                          "props": dump(getattr(msg, "properties", None), PUB_PROPS)}), flush=True)
        if len(got) >= count:
            done.set()
    c.on_connect, c.on_subscribe, c.on_disconnect, c.on_message = \
        on_connect, on_subscribe, on_disconnect, on_message
    c.connect("127.0.0.1", PORT, 30)
    c.loop_start()
    done.wait(timeout)
    c.loop_stop()
    print(json.dumps({"event": "end", "received": len(got)}), flush=True)

elif cmd == "pub":
    topic, payload = sys.argv[2], sys.argv[3]
    qos = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    retain = len(sys.argv) > 5 and sys.argv[5] == "retain"
    ver = sys.argv[6] if len(sys.argv) > 6 else "5"
    c, proto = client("interop-pub", ver)
    props = None
    if proto == MQTTProtocolVersion.MQTTv5:
        props = Properties(PacketTypes.PUBLISH)
        props.PayloadFormatIndicator = 1
        props.MessageExpiryInterval = 120
        props.ContentType = "application/json"
        props.ResponseTopic = "interop/response"
        props.CorrelationData = b"corr-1234"
        props.UserProperty = [("a", "1"), ("b", "two")]
    c.on_connect = lambda cl, u, f, rc, p=None: print("connack", rc, flush=True)
    c.connect("127.0.0.1", PORT, 30)
    c.loop_start()
    time.sleep(0.5)
    c.publish(topic, payload, qos=qos, retain=retain, properties=props).wait_for_publish(10)
    time.sleep(0.5)
    c.disconnect(); c.loop_stop()

else:
    sys.exit(__doc__)
```

</details>

<details>
<summary><code>interop.js</code> - mqtt.js driver</summary>

```javascript
// mqtt.js driver.  usage:
//   node interop.js sub <topic> [count] [timeout_ms] [5|4] [qos]
//   node interop.js pub <topic> <payload> [qos] [retain|-] [5|4]
// Env: MQTT_PORT (default 1893), NODE_PATH pointing at node_modules.
const mqtt = require('mqtt');
const [cmd, topic, a3, a4, a5, a6] = process.argv.slice(2);
const version = ((cmd === 'sub' ? a5 : a6) || '5') === '5' ? 5 : 4;
const conn = {protocolVersion: version, clientId: `node-${cmd}`, username: 'guest',
              password: 'guest', clean: true, reconnectPeriod: 0};
const c = mqtt.connect(`mqtt://127.0.0.1:${process.env.MQTT_PORT || 1893}`, conn);
const say = (o) => console.log(JSON.stringify(o, (k, v) =>
  (v && v.type === 'Buffer') ? Buffer.from(v.data).toString() : v));
c.on('error', (e) => say({event: 'error', error: String(e)}));
c.on('disconnect', (p) => say({event: 'disconnect', reasonCode: p.reasonCode}));

if (cmd === 'sub') {
  const count = parseInt(a3 || '1'), tmo = parseInt(a4 || '10000'), qos = parseInt(a6 || '1');
  let got = 0;
  const finish = () => { say({event: 'end', received: got}); c.end(true); process.exit(0); };
  const timer = setTimeout(finish, tmo);
  c.on('connect', (ack) => {
    say({event: 'connack', rc: ack.reasonCode, props: ack.properties || null});
    c.subscribe(topic, {qos}, (err, granted) =>
      say({event: 'suback', err: err ? String(err) : null, granted}));
  });
  c.on('message', (t, payload, packet) => {
    got++;
    say({event: 'message', topic: t, qos: packet.qos, retain: packet.retain,
         payload: payload.toString(), props: packet.properties || null});
    if (got >= count) { clearTimeout(timer); setTimeout(finish, 200); }
  });
} else {
  const qos = parseInt(a4 || '1'), retain = a5 === 'retain';
  const props = version === 5 ? {properties: {
    payloadFormatIndicator: true, messageExpiryInterval: 120,
    contentType: 'application/json', responseTopic: 'interop/response',
    correlationData: Buffer.from('corr-1234'), userProperties: {a: '1', b: 'two'}}} : {};
  c.on('connect', (ack) => {
    say({event: 'connack', rc: ack.reasonCode, props: ack.properties || null});
    c.publish(topic, a3 || 'hello', Object.assign({qos, retain}, props), (err) => {
      say({event: 'published', err: err ? String(err) : null});
      setTimeout(() => { c.end(true); process.exit(0); }, 300);
    });
  });
}
```

</details>

<details>
<summary><code>raw_v5.py</code> - hand-built packets for the byte-exact cases</summary>

```python
"""Hand-built v5 packets over a raw socket: exact-byte checks a library won't let us make."""
import os, socket, sys, time

PORT = int(os.environ.get("MQTT_PORT", "1893"))

def s16(b): return len(b).to_bytes(2, "big") + b
def varint(n):
    out = b""
    while True:
        d = n % 128; n //= 128
        out += bytes([d | (0x80 if n else 0)])
        if not n: return out
def pkt(t, flags, body): return bytes([(t << 4) | flags]) + varint(len(body)) + body

def connect(client_id, props=b"", will=None):
    flags = 0xC0  # username+password
    body = s16(b"MQTT") + bytes([5])
    if will:
        wtopic, wpayload, wqos, wretain = will
        flags |= 0x04 | (wqos << 3) | (0x20 if wretain else 0)
    body += bytes([flags]) + (30).to_bytes(2, "big")
    body += varint(len(props)) + props
    body += s16(client_id.encode())
    if will:
        body += varint(0)  # no will properties
        body += s16(wtopic.encode()) + s16(wpayload)
    body += s16(b"guest") + s16(b"guest")
    return pkt(1, 0, body)

def read_packet(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        h = sock.recv(1)
        if not h: return None
        ln, mult = 0, 1
        while True:
            b = sock.recv(1)[0]
            ln += (b & 127) * mult
            if not (b & 128): break
            mult *= 128
        body = b"" if ln == 0 else sock.recv(ln)
        return h + bytes([ln]) + body
    except (socket.timeout, IndexError, ConnectionResetError):
        return None

def describe(p):
    if p is None: return "no response / closed"
    t = p[0] >> 4
    names = {2: "CONNACK", 3: "PUBLISH", 4: "PUBACK", 9: "SUBACK", 11: "UNSUBACK", 13: "PINGRESP", 14: "DISCONNECT"}
    return f"{names.get(t, t)} bytes={p.hex()}"

def fresh(client_id, props=b"", will=None):
    s = socket.create_connection(("127.0.0.1", PORT), timeout=5)
    s.sendall(connect(client_id, props, will))
    return s, describe(read_packet(s))

case = sys.argv[1]

if case == "disconnect_forms":
    # subscriber that watches for a will
    ws, wack = fresh("raw-will-watcher")
    print("watcher connack:", wack)
    ws.sendall(pkt(8, 2, (1).to_bytes(2, "big") + varint(0) + s16(b"raw/will") + bytes([0])))
    print("watcher suback:", describe(read_packet(ws)))
    forms = {
        "no reason byte      (E0 00)": pkt(14, 0, b""),
        "reason 0x00 only":            pkt(14, 0, bytes([0x00])),
        "reason 0x00 + empty props":   pkt(14, 0, bytes([0x00]) + varint(0)),
        "reason 0x00 + expiry=30":     pkt(14, 0, bytes([0x00]) + varint(5) + bytes([0x11]) + (30).to_bytes(4, "big")),
        "reason 0x04 (with will)":     pkt(14, 0, bytes([0x04])),
    }
    for name, dp in forms.items():
        s, ack = fresh("raw-disc", will=("raw/will", b"WILL-FIRED", 1, False))
        s.sendall(dp)
        time.sleep(0.6)
        s.close()
        got = read_packet(ws, 1.0)
        print(f"  {name:30s} -> connack {ack[:8]}, will published: {'YES' if got else 'no'}")
    ws.close()

elif case == "empty_topic":
    s, ack = fresh("raw-empty-topic")
    print("connack:", ack)
    s.sendall(pkt(3, 0x02, s16(b"") + (1).to_bytes(2, "big") + varint(0) + b"x"))
    print("after empty-topic qos1 PUBLISH:", describe(read_packet(s)))
    s.close()

elif case == "tiny_max_packet_size":
    # maximum packet size = 5: our CONNACK is larger than that -> [MQTT-3.1.2-24]
    props = bytes([0x27]) + (5).to_bytes(4, "big")
    s, ack = fresh("raw-tiny-mps", props=props)
    print("connack with maximum-packet-size=5 requested:", ack)
    print("  connack size:", "n/a" if ack.startswith("no") else len(bytes.fromhex(ack.split("bytes=")[1])))
    s.close()

elif case == "oversized_suback":
    props = bytes([0x27]) + (12).to_bytes(4, "big")
    s, ack = fresh("raw-suback-mps", props=props)
    print("connack:", ack)
    filters = b""
    for i in range(10):
        filters += s16(f"raw/filter/{i}".encode()) + bytes([0])
    s.sendall(pkt(8, 2, (7).to_bytes(2, "big") + varint(0) + filters))
    print("suback for 10 filters under maximum-packet-size=12:", describe(read_packet(s)))
    s.close()

elif case == "packet_id_zero":
    s, ack = fresh("raw-pid-zero")
    print("connack:", ack)
    s.sendall(pkt(3, 0x02, s16(b"raw/pid") + (0).to_bytes(2, "big") + varint(0) + b"x"))
    print("qos1 PUBLISH with packet id 0:", describe(read_packet(s)))
    s.close()

if case == "second_connect":
    s, ack = fresh("raw-double-connect")
    print("first connack:", ack)
    s.sendall(connect("raw-double-connect"))
    print("after a second CONNECT [MQTT-3.1.0-2]:", describe(read_packet(s)))
    s.close()

if case == "server_packet_from_client":
    s, ack = fresh("raw-bad-packet")
    print("connack:", ack)
    s.sendall(pkt(13, 0, b""))  # PINGRESP: server-to-client only
    print("after a client-sent PINGRESP:", describe(read_packet(s)))
    s.close()

if case == "auth_packet":
    s, ack = fresh("raw-auth")
    print("connack:", ack)
    s.sendall(pkt(15, 0, bytes([0x18]) + varint(0)))  # AUTH, reason 0x18 continue
    print("after an AUTH packet on a plain connection:", describe(read_packet(s)))
    s.close()

if case == "pubrel":
    s, ack = fresh("raw-pubrel")
    print("connack:", ack)
    s.sendall(pkt(6, 2, (1).to_bytes(2, "big")))  # PUBREL
    print("after a stray PUBREL:", describe(read_packet(s)))
    s.close()
```

</details>

## Running the checks

### Capabilities

```sh
./broker.sh start results/broker.log
./venv/bin/python interop.py caps
```

Compare against `connection_factory.cr#build_server_capabilities`. Expected:
**no** `MaximumQoS` (QoS 2 is supported, and absent means 2), `RetainAvailable 1`,
`WildcardSubscriptionAvailable 1`,
`TopicAliasMaximum 0`, `SubscriptionIdentifierAvailable 0`,
`SharedSubscriptionAvailable 0`, `MaximumPacketSize 268435455`, and **no**
`ReceiveMaximum` (we do not advertise one, see `MQTT5-RELEASE-NOTES.md`).

### Property round-trips across codecs

Each pair should show all six properties on the receiving side. Publisher and
subscriber are deliberately different libraries.

```sh
p=./venv/bin/python
# paho -> paho
$p interop.py sub rt/1 1 12 5 1 & sleep 1.5; $p interop.py pub rt/1 hello 1 - 5; wait
# paho -> mqtt.js
node interop.js sub rt/2 1 12000 5 1 & sleep 1.5; $p interop.py pub rt/2 hello 1 - 5; wait
# mqtt.js -> paho
$p interop.py sub rt/3 1 12 5 1 & sleep 1.5; node interop.js pub rt/3 hello 1 - 5; wait
# mosquitto -> paho
$p interop.py sub rt/4 1 15 5 1 & sleep 1.5
docker run --rm --network host eclipse-mosquitto mosquitto_pub \
  -h 127.0.0.1 -p 1893 -u guest -P guest -V 5 -q 1 -t rt/4 -m hello \
  -D publish payload-format-indicator 1 \
  -D publish message-expiry-interval 120 \
  -D publish content-type application/json \
  -D publish response-topic interop/response \
  -D publish correlation-data corr-1234 \
  -D publish user-property a 1 -D publish user-property b two
wait
# cross-version: v5 publisher -> v3.1.1 subscriber (properties must vanish, payload must not)
$p interop.py sub rt/5 1 12 311 1 & sleep 1.5; $p interop.py pub rt/5 hello 1 - 5; wait
# retained: the replay must keep its properties (item F)
$p interop.py pub rt/6 retained 1 retain 5; $p interop.py sub rt/6 1 8 5 1
```

### One command per row of the compliance table

`-D` puts an arbitrary property on the wire, and `-d` prints the reason code that
comes back. `mos` below is
`docker run --rm --network host eclipse-mosquitto`.

| check | command | expected |
|---|---|---|
| QoS 2 publish | `mos mosquitto_pub ... -V 5 -d -q 2 -t x -m x` | PUBREC, PUBREL, PUBCOMP (#2236). Before QoS 2: refused client-side off our `maximum_qos`, DISCONNECT 155 (`0x9B`) if forced |
| Topic Alias | `... mosquitto_pub -V 5 -d -q 1 -t x -m x -D publish topic-alias 1` | DISCONNECT 148 (`0x94`) |
| Shared subscription | `... mosquitto_sub -V 5 -d -W 5 -t '$share/g1/x'` | DISCONNECT 158 (`0x9E`) |
| Subscription Identifier | `... mosquitto_sub -V 5 -d -W 5 -t x -D subscribe subscription-identifier 1` | DISCONNECT 161 (`0xA1`) |
| Enhanced auth | `... mosquitto_pub -V 5 -d -t x -m x -D connect authentication-method SCRAM-SHA-1` | CONNACK 140 (`0x8C`) |
| No credentials | `... mosquitto_pub -V 5 -d -t x -m x` (drop `-u`/`-P`) | CONNACK 135 (`0x87`) |
| Maximum Packet Size on delivery | `... mosquitto_sub -V 5 -d -W 8 -q 1 -t x -D connect maximum-packet-size 40`, then publish 200 bytes | no PUBLISH arrives, connection stays up |
| Delivery QoS | `... mosquitto_sub -V 5 -d -W 8 -q 1 -t x`, then `mosquitto_pub -V 5 -q 0 -t x -m x` | the delivered PUBLISH is QoS **0**, not 1 [MQTT-3.8.4-8]. Repeat with `-V 311` |
| Will QoS 2 | `interop.py` with `will_set(..., qos=2)` | CONNACK Success, now that QoS 2 is supported. Was Success at the 2026-08-19 run too, then `0x9B` until QoS 2 |
| Session Expiry 0 | `... mosquitto_sub -V 5 -c -x 0 -i c1 -q 1 -t x`, publish while offline, reconnect | **nothing arrives**: expiry 0 ends the session with the connection [MQTT-3.1.2-11] |
| Session Expiry non-zero | same with `-x 60` | the message arrives, and `mqtt.c1` is still there between connections |
| Clean Start 1 + expiry | `... mosquitto_sub -V 5 -C 1 -x 60 -i c2 -q 1 -t x` | old session discarded, new one persists - the case the Paho suite used to fail |

### Byte-exact cases

```sh
for c in disconnect_forms empty_topic tiny_max_packet_size oversized_suback \
         packet_id_zero second_connect server_packet_from_client auth_packet pubrel; do
  echo "== $c"; python3 raw_v5.py "$c"
done
```

`empty_topic`, `second_connect`, `server_packet_from_client` and `auth_packet`
each exercise the item J2 path and must print a `DISCONNECT` whose last byte is
`0x82`. `pubrel` used to be one of them; with QoS 2 a stray PUBREL is legal and
gets `PUBCOMP` with reason `0x92` (`7003000192`).

`tiny_max_packet_size` must print no CONNACK, and `oversized_suback` no SUBACK:
since item I a packet over the client's limit is not sent, and the connection
closes. `packet_id_zero` must print a `DISCONNECT`
ending in `0x82`; before item L it got a PUBACK carrying id 0.

`disconnect_forms` is the one to keep an eye on: it sends five DISCONNECT
encodings (no reason byte, reason only, reason plus empty properties, reason plus a
session-expiry property, and `0x04`) and reports whether the will fired. Expected:
`0x04` publishes it, and so does the session-expiry form. Its CONNECT sent no
interval, so naming one on DISCONNECT is a Protocol Error that is answered with
`0x82` and does not count as a valid DISCONNECT (§3.14.2.2.2). The other three
must not.

### After every run

```sh
grep -ril "unhandled exception\|invalid memory access\|BUG:" results/   # must print nothing
cat results/*/*.broker.log | grep -oE "(WARN|ERROR) .*" | sed 's/\[[^]]*\]//g' | sort | uniq -c | sort -rn
```

The second command is the fastest way to spot a bad rejection path: every
`Protocol violation, disconnecting client: <ReasonCode>` line is a WARN by design,
so an `ERROR ... Read Loop error` in that list is a packet we mishandle rather than
reject. Since J2 landed there should be **none** - that count is now a regression
check, not a known gap.

## Interpreting the score

Do not read the raw pass count.

| run | 2026-08-19 | 2026-10-02 |
|---|---|---|
| v5 | 6 / 18 / 3 timeout | **15** / 11 / 1 timeout |
| v3.1.1 | 3 / 6 | **7** / 2 |

Before QoS 2 the suite was also run from a copy with every QoS 2 use lowered to
QoS 1, which scored 8 / 18 / 1 on v5 and 7 / 2 on v3.1.1 on 2026-08-19. The
2026-10-02 run is the suite as published, against `9e3d3559` with QoS 2 from
#2236. Every remaining v5 failure maps to something known:

| test | why |
|---|---|
| `test_retained_message` | item F: a retained message lost its User Property. Fixed since, not yet re-run |
| `test_subscribe_options` | item F: retained replays came back at the subscription QoS, not the publisher's [MQTT-3.8.4-8]. Fixed since, not yet re-run |
| `test_publication_expiry` | item M: Message Expiry Interval was carried but never enforced. Fixed since, not yet re-run |
| `test_will_delay` | item E |
| `test_flow_control1`, `test_flow_control2` (timeout) | item N: the client's Receive Maximum was not honoured. Fixed since, not yet re-run |
| `test_dollar_topics` | item O: `#` matches `$`-prefixed topics [MQTT-4.7.2-1]; its own PR |
| `test_subscribe_identifiers`, `test_shared_subscriptions` | correct rejections (`0xA1`, `0x9E`) the test client cannot cope with |
| the three below | harness assumptions, fine to fail |

The two v3.1.1 failures are `test_subscribe_failure`, a harness assumption, and
`test_dollar_topics`, item O. The assumptions:

- `test_subscribe_failure` wants an ACL denying `test/nosubscribe`;
  `mqtt.permission_check_enabled` is false by default, so we grant it.
- `test_server_keep_alive` wants a `ServerKeepAlive` property; a server MAY send
  one and we do not.
- `test_server_topic_alias` wants the server to use aliases; we advertise
  `topic_alias_maximum = 0` and never do, which is legal.

The v3.1.1 row is the cleanest single signal: 7 of 9 as published, matching what
the QoS-lowered copy scored on 2026-08-19, with the same two failures.
