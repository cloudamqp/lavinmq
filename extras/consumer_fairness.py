#!/usr/bin/env python3
"""Consumer fairness scenarios against a running broker. usage: consumer_fairness.py PORT [scenario ...]"""
import sys, threading, time, statistics, pika

PORT = int(sys.argv[1])
ONLY = sys.argv[2:]
params = pika.ConnectionParameters("localhost", PORT, heartbeat=0)


def longest_run(seq):
    best = cur = 0
    prev = None
    for x in seq:
        cur = cur + 1 if x == prev else 1
        best = max(best, cur)
        prev = x
    return best


def summary(got, tags):
    n = max(len(got), 1)
    shares = " ".join(f"{t}={got.count(t)/n:.2f}" for t in tags)
    return f"n={len(got)} shares {shares} longest_run={longest_run(got)}"


class Consumer(threading.Thread):
    """One consumer on its own connection (or a shared channel factory)."""

    def __init__(self, q, tag, prefetch, got, arrived, work=0.0, no_ack=False,
                 args=None, global_qos=False, action="ack", lat=None):
        super().__init__(daemon=True)
        self.q, self.tag, self.prefetch, self.got, self.arrived = q, tag, prefetch, got, arrived
        self.work, self.no_ack, self.args, self.global_qos = work, no_ack, args, global_qos
        self.action, self.lat = action, lat
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.paused_acks = False
        self.held = []

    def on_message(self, ch, method, props, body):
        if self.work:
            time.sleep(self.work)
        self.got.append(self.tag)
        if self.lat is not None:
            self.lat.append(time.time() - float(body))
        if not self.no_ack:
            act = self.action(self, body) if callable(self.action) else self.action
            if act == "ack":
                ch.basic_ack(method.delivery_tag)
            elif act == "requeue":
                ch.basic_nack(method.delivery_tag, requeue=True)
            elif act == "hold":
                self.held.append(method.delivery_tag)
        self.arrived.release()

    def run(self):
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()
        if self.prefetch:
            self.ch.basic_qos(prefetch_count=self.prefetch, global_qos=self.global_qos)
        self.ch.basic_consume(self.q, self.on_message, auto_ack=self.no_ack,
                              arguments=self.args, consumer_tag=self.tag)
        self.ready.set()
        try:
            while not self.stop.is_set():
                self.conn.process_data_events(time_limit=0.02)
            self.conn.close()
        except Exception as e:  # connection killed by a scenario
            self.err = e


def setup(q, args=None):
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    try:
        ch.queue_delete(q)
    except Exception:
        ch = conn.channel()
    ch.queue_declare(q, arguments=args)
    return conn, ch


def start(consumers):
    for c in consumers:
        c.start()
        c.ready.wait(5)
    time.sleep(0.5)


def stop(consumers):
    for c in consumers:
        c.stop.set()
    for c in consumers:
        c.join(2)


def one_at_a_time(name, specs, n=200, **kw):
    q = "fair-" + name
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, t, p, got, arrived, **kw) for t, p in specs]
    start(cs)
    for i in range(n):
        pub.basic_publish("", q, str(time.time()).encode())
        arrived.acquire(timeout=5)
    stop(cs)
    conn.close()
    print(f"{name:38s} {summary(got, [t for t, _ in specs])}")
    return got


def s_issue():
    one_at_a_time("issue-8-4", [("A", 8), ("B", 4)])


def s_same_channel():
    q = "fair-same-channel"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cconn = pika.BlockingConnection(params)
    ch = cconn.channel()
    ch.basic_qos(prefetch_count=8)

    def mk(tag):
        def cb(ch_, m, p, b):
            ch_.basic_ack(m.delivery_tag)
            got.append(tag)
            arrived.release()
        return cb
    for t in "AB":
        ch.basic_consume(q, mk(t), consumer_tag=t)
    stopf = threading.Event()

    def loop():
        while not stopf.is_set():
            cconn.process_data_events(time_limit=0.02)
    th = threading.Thread(target=loop, daemon=True)
    th.start()
    time.sleep(0.5)
    for i in range(200):
        pub.basic_publish("", q, b"x")
        arrived.acquire(timeout=5)
    stopf.set()
    th.join()
    print(f"{'same-channel':38s} {summary(got, 'AB')}")


def s_five():
    one_at_a_time("five-consumers", [("A", 10), ("B", 5), ("C", 2), ("D", 100), ("E", 0)], n=300)


def s_noack():
    one_at_a_time("no-ack", [("A", 0), ("B", 0), ("C", 0)], n=300, no_ack=True)


def s_latency():
    q = "fair-latency"
    conn, pub = setup(q)
    got, arrived, lat = [], threading.Semaphore(0), []
    cs = [Consumer(q, t, p, got, arrived, work=0.04, lat=lat) for t, p in (("A", 8), ("B", 4))]
    start(cs)
    for i in range(300):
        pub.basic_publish("", q, str(time.time()).encode())
        conn.sleep(0.03)
    time.sleep(2)
    stop(cs)
    lat.sort()
    print(f"{'latency 30ms pub/40ms work':38s} {summary(got, 'AB')} "
          f"median={statistics.median(lat)*1000:.0f}ms p95={lat[int(len(lat)*.95)]*1000:.0f}ms")


def s_burst():
    q = "fair-burst"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, t, p, got, arrived, work=0.005) for t, p in (("A", 8), ("B", 4), ("C", 8))]
    start(cs)
    for i in range(600):
        pub.basic_publish("", q, b"x")
    for i in range(600):
        arrived.acquire(timeout=5)
    stop(cs)
    print(f"{'burst 600, 5ms work':38s} {summary(got, 'ABC')}")


def s_small_bursts():
    """Bursts of 3 messages with idle gaps, prefetch 10 each: who gets a burst?"""
    q = "fair-small-bursts"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, t, 10, got, arrived) for t in "ABC"]
    start(cs)
    for i in range(100):
        for _ in range(3):
            pub.basic_publish("", q, b"x")
        for _ in range(3):
            arrived.acquire(timeout=5)
    stop(cs)
    print(f"{'bursts of 3, idle between':38s} {summary(got, 'ABC')}")


def s_requeue():
    """A always nack-requeues; B and C ack. Does the rejected message move on?"""
    q = "fair-requeue"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, "A", 8, got, arrived, action="requeue"),
          Consumer(q, "B", 8, got, arrived), Consumer(q, "C", 8, got, arrived)]
    start(cs)
    t0 = time.time()
    done = 0
    for i in range(100):
        pub.basic_publish("", q, b"x")
        # wait until an acking consumer got it
        while time.time() - t0 < 60:
            arrived.acquire(timeout=5)
            if got[-1] != "A" or got.count("B") + got.count("C") > done:
                if got.count("B") + got.count("C") > done:
                    done = got.count("B") + got.count("C")
                    break
    stop(cs)
    print(f"{'poison consumer A nack-requeues':38s} {summary(got, 'ABC')} acked={done}/100 "
          f"deliveries_to_A_per_msg={got.count('A')/100:.2f}")


def s_priority():
    """H (x-priority 10, prefetch 2) must get everything while it has capacity."""
    q = "fair-priority"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    lo = [Consumer(q, t, 8, got, arrived) for t in ("L1", "L2")]
    start(lo)
    # one message so the low priority consumers are in line before H exists
    pub.basic_publish("", q, b"x"); arrived.acquire(timeout=5)
    pub.basic_publish("", q, b"x"); arrived.acquire(timeout=5)
    hi = Consumer(q, "H", 2, got, arrived, args={"x-priority": 10})
    start([hi])
    del got[:]
    for i in range(100):
        pub.basic_publish("", q, b"x")
        arrived.acquire(timeout=5)
    part1 = list(got)
    # now H holds its messages: it fills up (2), the rest must alternate L1/L2
    del got[:]
    hi.action = "hold"
    for i in range(42):
        pub.basic_publish("", q, b"x")
        arrived.acquire(timeout=5)
    part2 = list(got)
    # H acks -> capacity again -> must get everything again
    del got[:]
    hi.action = "ack"
    held, hi.held = hi.held, []
    for t in held:
        hi.conn.add_callback_threadsafe(lambda t=t: hi.ch.basic_ack(t))
    time.sleep(0.5)
    for i in range(100):
        pub.basic_publish("", q, b"x")
        arrived.acquire(timeout=5)
    part3 = list(got)
    stop(lo + [hi])
    tags = ["H", "L1", "L2"]
    print(f"{'priority: H has capacity':38s} {summary(part1, tags)}")
    print(f"{'priority: H full (2 held)':38s} {summary(part2, tags)}")
    print(f"{'priority: H capacity again':38s} {summary(part3, tags)}")


def s_global_prefetch():
    """Two consumers per channel under a global prefetch, across two channels."""
    q = "fair-global"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, t, 4, got, arrived, global_qos=True) for t in "ABC"]
    start(cs)
    for i in range(300):
        pub.basic_publish("", q, b"x")
        arrived.acquire(timeout=5)
    stop(cs)
    print(f"{'global prefetch 4, one at a time':38s} {summary(got, 'ABC')}")


def s_churn():
    """Consumers are cancelled / killed while waiting in line; the rest keep getting messages."""
    q = "fair-churn"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, t, 8, got, arrived) for t in "ABCD"]
    start(cs)
    lost = 0
    for i in range(400):
        if i == 100:  # graceful cancel
            cs[0].conn.add_callback_threadsafe(lambda: cs[0].ch.basic_cancel("A"))
            time.sleep(0.2)
        if i == 200:  # abrupt socket close
            cs[1].conn.add_callback_threadsafe(lambda: cs[1].conn._impl._adapter_disconnect_stream())
            time.sleep(0.2)
        if i == 300:  # new consumer joins
            cs.append(Consumer(q, "E", 8, got, arrived))
            start(cs[-1:])
        pub.basic_publish("", q, b"x")
        if not arrived.acquire(timeout=5):
            lost += 1
    for a, b in ((0, 100), (100, 200), (200, 300), (300, 400)):
        print(f"{'churn msgs %d-%d' % (a, b):38s} {summary(got[a:b], 'ABCDE')}")
    print(f"{'churn':38s} undelivered within 5s: {lost}")
    stop(cs)


def s_flow():
    """channel.flow(false) on the consumer first in line must not block the others."""
    q = "fair-flow"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, t, 8, got, arrived) for t in "ABC"]
    start(cs)
    for i in range(30):
        pub.basic_publish("", q, b"x"); arrived.acquire(timeout=5)
    cs[0].conn.add_callback_threadsafe(lambda: cs[0].ch.flow(False))
    time.sleep(0.3)
    del got[:]
    lost = 0
    for i in range(100):
        pub.basic_publish("", q, b"x")
        lost += not arrived.acquire(timeout=5)
    part = list(got)
    cs[0].conn.add_callback_threadsafe(lambda: cs[0].ch.flow(True))
    time.sleep(0.3)
    del got[:]
    for i in range(99):
        pub.basic_publish("", q, b"x")
        lost += not arrived.acquire(timeout=5)
    time.sleep(0.5)
    print(f"{'flow: A flow=false':38s} {summary(part, 'ABC')}")
    print(f"{'flow: A flow=true again':38s} {summary(got, 'ABC')} undelivered={lost}")
    stop(cs)


def s_delete():
    """Queue deleted / purged while consumers wait in line; then a fresh queue works."""
    q = "fair-delete"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, t, 8, got, arrived) for t in "ABC"]
    start(cs)
    for i in range(30):
        pub.basic_publish("", q, b"x"); arrived.acquire(timeout=5)
    pub.queue_purge(q)
    for i in range(30):
        pub.basic_publish("", q, b"x"); arrived.acquire(timeout=5)
    n1 = len(got)
    pub.queue_delete(q)
    time.sleep(0.5)
    stop(cs)
    del got[:]
    pub.queue_declare(q)
    cs = [Consumer(q, t, 8, got, arrived) for t in "ABC"]
    start(cs)
    for i in range(60):
        pub.basic_publish("", q, b"x"); arrived.acquire(timeout=5)
    stop(cs)
    print(f"{'delete+redeclare':38s} before={n1}/60 after: {summary(got, 'ABC')}")


def s_idle():
    """Message interval longer than the 30s deliver loop idle timeout."""
    q = "fair-idle"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, t, 8, got, arrived) for t in "ABC"]
    start(cs)
    for i in range(8):
        pub.basic_publish("", q, b"x")
        arrived.acquire(timeout=5)
        conn.sleep(33)
    stop(cs)
    print(f"{'one msg every 33s (idle timeout 30s)':38s} {summary(got, 'ABC')} order={''.join(got)}")


def s_idle_mixed():
    """Interval 12s with 3 consumers: each waits 36s for its turn, past the 30s idle timeout."""
    q = "fair-idle-mixed"
    conn, pub = setup(q)
    got, arrived = [], threading.Semaphore(0)
    cs = [Consumer(q, t, 8, got, arrived) for t in "ABC"]
    start(cs)
    for i in range(18):
        pub.basic_publish("", q, b"x")
        arrived.acquire(timeout=5)
        conn.sleep(12)
    stop(cs)
    print(f"{'one msg every 12s, 3 consumers':38s} {summary(got, 'ABC')} order={''.join(got)}")


ALL = {k[2:]: v for k, v in list(globals().items()) if k.startswith("s_")}
for name in (ONLY or [k for k in ALL if not k.startswith("idle")]):
    try:
        ALL[name]()
    except Exception as e:
        print(f"{name:38s} ERROR {type(e).__name__}: {e}")
    sys.stdout.flush()
