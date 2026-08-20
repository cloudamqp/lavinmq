# MQTT interop harness - how to re-run the external verification

Companion to `MQTT5.md`. Section 7 there records *what* the 2026-08-19 external
run found; this file is *how to run it again*. Nothing here is wired into `make`
or CI on purpose: it needs a built binary, a network clone and a Docker pull, and
it is a release-gate check, not a per-commit one.

Re-run it after B / D / E / F land - most of the currently failing Paho tests are
the grading function for exactly those items. The delivery-QoS and DISCONNECT
`0x82` expectations below were updated when J1/J2 were fixed but have **not** been
confirmed by a re-run yet.

## What it exercises that our own specs cannot

`spec/mqtt` drives the broker through `MQTT::Protocol::IO::V5` - the same codec
the broker encodes with - so a self-consistent wire-format mistake is invisible to
it. These tools bring their own codecs:

| tool | what it is good for |
|---|---|
| Eclipse Paho interoperability suite | 27 v5 + 9 v3.1.1 broker conformance tests, written against the spec by the people who wrote the reference client |
| paho-mqtt (Python) | property round-trips, capability inspection, wills |
| mqtt.js (Node) | a third independent codec |
| mosquitto clients | `-D` sets any v5 property by hand, so one command per row of the section 2 compliance table; prints the DISCONNECT reason code it receives |
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

# 3. the real client libraries
python3 -m venv venv && ./venv/bin/pip -q install 'paho-mqtt>=2,<3'
mkdir -p node && ( cd node && npm init -y >/dev/null && npm install --silent mqtt )
docker pull -q eclipse-mosquitto
export NODE_PATH="$W/node/node_modules"
```

### The QoS-clamped copy of the suite

The unmodified v5 suite cannot grade this broker: 22 of its 27 tests use QoS 2
somewhere, and its client ignores our advertised `maximum_qos = 1` (a
[MQTT-3.2.2-11] violation on the client's side). We correctly answer DISCONNECT
`0x9B` and close, after which several tests spin forever on
`while len(callback.messages) < 3`. Run the unmodified suite once to confirm that
rejection path, then run this copy for everything else.

```sh
cp -a paho.mqtt.testing paho.qos1
python3 - <<'EOF'
import re
p = "paho.qos1/interoperability/client_test5.py"
s = open(p).read()
s = s.replace("SubscribeOptions(2)", "SubscribeOptions(1)")
s = re.sub(r"(\.publish\([^\n]*?),\s*2(\s*[,)])", r"\1, 1\2", s)
s = s.replace("1 in qoss and 2 in qoss and 0 in qoss", "1 in qoss and 0 in qoss")
open(p, "w").write(s)

p = "paho.qos1/interoperability/client_test.py"
s = open(p).read()
s = re.sub(r"(\.publish\([^\n]*?),\s*2(\s*[,)])", r"\1, 1\2", s)
s = s.replace(", [2])", ", [1])").replace(", [2, 2])", ", [1, 1])").replace(", [2, 1])", ", [1, 1])")
s = s.replace("assert callback.messages[0][2] == 2", "assert callback.messages[0][2] == 1")
s = s.replace("assert (callback.messages[0][2] == 2 and callback.messages[1][2] == 1) or \\",
              "assert (callback.messages[0][2] == 1 and callback.messages[1][2] == 1) or \\")
# client_test.py forgets to strip -p from argv, so unittest chokes on it
old = '''    elif o in ("-p", "--port"):
      port = int(a)'''
s = s.replace(old, old + '''
      sys.argv.remove("-p") if "-p" in sys.argv else sys.argv.remove("--port")
      sys.argv.remove(a)''', 1)
open(p, "w").write(s)

# the vendored v5 client's connect() defaults to willQoS=2, so every test that
# sets a will asks for one we advertise we cannot serve
p = "paho.qos1/interoperability/mqtt/clients/V5/main.py"
s = open(p).read().replace("willQoS=2", "willQoS=1")
open(p, "w").write(s)
EOF
```

Four kinds of edit, 96 lines in `client_test5.py`, all mechanical: `SubscribeOptions(2)`
-> `(1)`, the third positional argument of `.publish()` from `2` to `1`, one
`assertTrue(1 in qoss and 2 in qoss and 0 in qoss)` that can no longer hold, and
the `willQoS` default. Nothing about what is being asserted changes.

## The scripts

Write these four files into `$W`.

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

case "${1:-}" in
start)
  LOG="${2:-$W/broker.log}"
  "$0" stop
  rm -rf "$DATA"; mkdir -p "$DATA"
  "$BIN" --data-dir "$DATA" --bind 127.0.0.1 \
         --mqtt-port 1883 --amqp-port 5673 --http-port 15673 \
         --mqtts-port -1 --amqps-port -1 --debug > "$LOG" 2>&1 &
  echo $! > "$PIDFILE"
  for _ in $(seq 1 100); do
    if (exec 3<>/dev/tcp/127.0.0.1/1883) 2>/dev/null; then
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

`$LMQ` is the worktree exported during setup. Ports 1883 / 5673 / 15673 keep it clear of a
default-port LavinMQ. A **fresh data dir per run** is not optional: retained
messages and persisted sessions leak between suites otherwise.

To run a second broker in parallel (useful: one suite on 1883 while you poke at
1884), copy the script and change `DATA`, `PIDFILE`, all three ports, **and** give
it its own `--metrics-http-port` plus `--control-unix-path`, `--amqp-unix-path`,
`--http-unix-path`. Two instances otherwise fight over `/tmp/lavinmqctl.sock` and
the second dies. Keep those socket paths short - a long path trips the 107-byte
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
  timeout 180 python3 -u "$SUITE" "Test.$t" > "$OUT/$t.out" 2>&1
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
./run_suite.sh paho.mqtt.testing client_test5.py results/v5        # as published
./run_suite.sh paho.qos1        client_test5.py results/v5-qos1    # the useful run
./run_suite.sh paho.qos1        client_test.py  results/v3-qos1    # 7/9 as of 2026-08-19
```

Each test leaves `results/<dir>/<test>.out` (client side) next to
`<test>.broker.log` (server side, `--debug`). Read them as a pair: "client hung"
plus `WARN Protocol violation ... QoSNotSupported` is a correct rejection, not a
bug.

</details>

<details>
<summary><code>interop.py</code> - paho-mqtt driver (capabilities, pub, sub)</summary>

```python
#!/usr/bin/env python3
"""paho-mqtt driver for LavinMQ MQTT interop checks.

  interop.py caps                                   # dump CONNACK capabilities
  interop.py sub <topic> [count] [timeout] [5|311] [qos] [nl,rap,rh=N]
  interop.py pub <topic> <payload> [qos] [retain|-] [5|311]

Env: MQTT_PORT (default 1883). Credentials are always guest/guest.
Subscriber prints one JSON line per event, so callers can grep for "props".
"""
import json, os, sys, threading, time
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.subscribeoptions import SubscribeOptions

PORT = int(os.environ.get("MQTT_PORT", "1883"))
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
// Env: MQTT_PORT (default 1883), NODE_PATH pointing at node_modules.
const mqtt = require('mqtt');
const [cmd, topic, a3, a4, a5, a6] = process.argv.slice(2);
const version = ((cmd === 'sub' ? a5 : a6) || '5') === '5' ? 5 : 4;
const conn = {protocolVersion: version, clientId: `node-${cmd}`, username: 'guest',
              password: 'guest', clean: true, reconnectPeriod: 0};
const c = mqtt.connect(`mqtt://127.0.0.1:${process.env.MQTT_PORT || 1883}`, conn);
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

PORT = int(os.environ.get("MQTT_PORT", "1883"))

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
MQTT_PORT=1883 ./venv/bin/python interop.py caps
```

Compare against `connection_factory.cr#build_server_capabilities`. Expected:
`MaximumQoS 1`, `RetainAvailable 1`, `WildcardSubscriptionAvailable 1`,
`TopicAliasMaximum 0`, `SubscriptionIdentifierAvailable 0`,
`SharedSubscriptionAvailable 0`, `MaximumPacketSize 268435455`, and **no**
`ReceiveMaximum` (we do not advertise one - section 6).

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
  -h 127.0.0.1 -p 1883 -u guest -P guest -V 5 -q 1 -t rt/4 -m hello \
  -D publish payload-format-indicator 1 \
  -D publish message-expiry-interval 120 \
  -D publish content-type application/json \
  -D publish response-topic interop/response \
  -D publish correlation-data corr-1234 \
  -D publish user-property a 1 -D publish user-property b two
wait
# cross-version: v5 publisher -> v3.1.1 subscriber (properties must vanish, payload must not)
$p interop.py sub rt/5 1 12 311 1 & sleep 1.5; $p interop.py pub rt/5 hello 1 - 5; wait
# retained: item F - the store keeps only the body, so props come back empty
$p interop.py pub rt/6 retained 1 retain 5; $p interop.py sub rt/6 1 8 5 1
```

### One command per row of the compliance table

`-D` puts an arbitrary property on the wire, and `-d` prints the reason code that
comes back. `mos` below is
`docker run --rm --network host eclipse-mosquitto`.

| check | command | expected |
|---|---|---|
| QoS 2 publish | `mos mosquitto_pub ... -V 5 -d -q 2 -t x -m x` | mosquitto refuses client-side off our `maximum_qos`; force it past that and you get DISCONNECT 155 (`0x9B`) |
| Topic Alias | `... mosquitto_pub -V 5 -d -q 1 -t x -m x -D publish topic-alias 1` | DISCONNECT 148 (`0x94`) |
| Shared subscription | `... mosquitto_sub -V 5 -d -W 5 -t '$share/g1/x'` | DISCONNECT 158 (`0x9E`) |
| Subscription Identifier | `... mosquitto_sub -V 5 -d -W 5 -t x -D subscribe subscription-identifier 1` | DISCONNECT 161 (`0xA1`) |
| Enhanced auth | `... mosquitto_pub -V 5 -d -t x -m x -D connect authentication-method SCRAM-SHA-1` | CONNACK 140 (`0x8C`) |
| No credentials | `... mosquitto_pub -V 5 -d -t x -m x` (drop `-u`/`-P`) | CONNACK 135 (`0x87`) |
| Maximum Packet Size on delivery | `... mosquitto_sub -V 5 -d -W 8 -q 1 -t x -D connect maximum-packet-size 40`, then publish 200 bytes | no PUBLISH arrives, connection stays up |
| Delivery QoS | `... mosquitto_sub -V 5 -d -W 8 -q 1 -t x`, then `mosquitto_pub -V 5 -q 0 -t x -m x` | the delivered PUBLISH is QoS **0**, not 1 [MQTT-3.8.4-8]. Repeat with `-V 311` |
| Will QoS 2 | `interop.py` with `will_set(..., qos=2)` | CONNACK Success today - item I, spec 3.1.2.6 wants `0x9B` |
| Session Expiry 0 | `... mosquitto_sub -V 5 -c -x 0 -i c1 -q 1 -t x`, publish while offline, reconnect | the message arrives, i.e. the session outlived its zero expiry - item J3 |

### Byte-exact cases

```sh
for c in disconnect_forms empty_topic tiny_max_packet_size oversized_suback \
         packet_id_zero second_connect server_packet_from_client auth_packet pubrel; do
  echo "== $c"; MQTT_PORT=1883 python3 raw_v5.py "$c"
done
```

`second_connect`, `server_packet_from_client`, `auth_packet` and `pubrel` each
exercise the item J2 path and must now print a `DISCONNECT` whose last byte is
`0x82`, where they used to print "no response / closed".

`disconnect_forms` is the one to keep an eye on: it sends all five legal DISCONNECT
encodings (no reason byte, reason only, reason plus empty properties, reason plus a
session-expiry property, and `0x04`) and reports whether the will fired. Only
`0x04` may publish it.

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

Do not read the raw pass count. On 2026-08-19 the numbers were:

| run | PASS | FAIL | TIMEOUT |
|---|---|---|---|
| v5 as published | 6 | 18 | 3 |
| v5 QoS-clamped | 8 | 18 | 1 |
| v3.1.1 as published | 3 | 6 | 0 |
| v3.1.1 QoS-clamped | **7** | 2 | 0 |

Every v5 failure mapped to a documented item (B, D, E, F, Receive Maximum, shard
N3/O2), to a correct rejection the test client cannot cope with, or to one of
these harness assumptions:

- `test_subscribe_failure` wants an ACL denying `test/nosubscribe`;
  `mqtt.permission_check_enabled` is false by default, so we grant it.
- `test_server_keep_alive` wants a `ServerKeepAlive` property; a server MAY send
  one and we do not.
- `test_server_topic_alias` wants the server to use aliases; we advertise
  `topic_alias_maximum = 0` and never do, which is legal.
- `test_dollar_topics` wants `#` not to match `$`-prefixed topics; spec 4.7.2 is a
  SHOULD NOT, and we do match. Pre-existing on v3 too.

The v3.1.1 QoS-clamped row is the cleanest single signal: 7 of 9, with the two
failures being `test_subscribe_failure` and `test_dollar_topics` from that list.
