#!/bin/bash
# run.sh VARIANT SCENARIO REP -> one CSV row on stdout
# VARIANT: stock | fsd1 | fsd6 | floor8m | floor64m | auto1 | auto05
set -u
V=$1; S=$2; R=$3
SP=${SP:-/tmp/gc-adaptive}
cd /home/user/lavinmq
DATA=$SP/data; rm -rf $DATA; mkdir -p $DATA
envs=()
case $V in
  stock) ;;
  fsd1) envs=(GC_FREE_SPACE_DIVISOR=1) ;;
  fsd6) envs=(GC_FREE_SPACE_DIVISOR=6) ;;
  floor8m) envs=(LAVINMQ_GC_MIN_BYTES=$((8<<20))) ;;
  floor64m) envs=(LAVINMQ_GC_MIN_BYTES=$((64<<20))) ;;
  auto1) envs=(LAVINMQ_GC_AUTOTUNE=1) ;;
  auto05) envs=(LAVINMQ_GC_AUTOTUNE=0.5) ;;
  *) echo "bad variant" >&2; exit 1 ;;
esac
env GC_UNMAP_THRESHOLD=1 "${envs[@]}" taskset -c 0,1 bin/lavinmq --data-dir $DATA --log-level=warn > $SP/res/broker-$V-$S-$R.log 2>&1 &
BP=$!
until curl -fs -u guest:guest localhost:15672/api/overview >/dev/null 2>&1; do sleep 0.2; done
if [ -n "${BIG:-}" ]; then
  P0="taskset -c 2,3 /home/user/lavinmq/bin/lavinmqperf"
  sleep 3600 | $P0 queue-count -q 10000 -n > $SP/res/qc.log 2>&1 & QC=$!
  until grep -q "10000 queues" $SP/res/qc.log; do sleep 0.5; done
  sleep 3600 | $P0 connection-count -x 2000 -C 1 > $SP/res/cc.log 2>&1 & CC=$!
  until grep -q "2000 connections" $SP/res/cc.log; do sleep 0.5; done
  pkill -f "lavinmqperf queue-count"
  S0=$S; S=big-$S
  sleep 5
fi
gc() { curl -fs -u guest:guest localhost:15672/api/nodes/gc_stats; }
cpu() { awk '{print $14+$15}' /proc/$BP/stat; }
c0=$(cpu); g0=$(gc)
P="taskset -c 2,3 bin/lavinmqperf"
case ${S#big-} in
  small)  out=$($P throughput -x 2 -y 2 -s 256 -z 30 -q -j 2>&1); ops=$(grep -o '"avg_consume_rate":[0-9]*' <<<"$out" | cut -d: -f2) ;;
  large)  out=$($P throughput -x 1 -y 1 -s 262144 -z 30 -q -j 2>&1); ops=$(grep -o '"avg_consume_rate":[0-9]*' <<<"$out" | cut -d: -f2) ;;
  fanout) out=$($P throughput -x 1 -y 8 -s 1024 -e amq.fanout -z 30 -q -j 2>&1); ops=$(grep -o '"avg_consume_rate":[0-9]*' <<<"$out" | cut -d: -f2) ;;
  conn)   out=$($P connection-churn 2>&1); ops=$(grep -oE '^ *open-close[^(]*' <<<"$out" | awk '{print $(NF)}') ;;
  queue)  out=$($P queue-churn 2>&1); ops=$(grep -oE '^ *[a-z-]+ +[0-9.]+[kM]?' <<<"$out" | tail -1 | awk '{print $NF}') ;;
esac
c1=$(cpu); g1=$(gc)
hwm=$(awk '/VmHWM/{print $2}' /proc/$BP/status)
# idle period: does the heap/RSS come back down?
sleep 20
g2=$(gc); rss_idle=$(awk '/VmRSS/{print $2}' /proc/$BP/status)
pkill -f "lavinmqperf connection-count"; pkill -f "sleep 3600"
kill $BP; wait $BP 2>/dev/null
python3 -I - "$V" "$S" "$R" "$ops" "$c0" "$c1" "$hwm" "$rss_idle" "$g0" "$g1" "$g2" <<'PY'
import sys, json
V,S,R,ops,c0,c1,hwm,rssi,g0,g1,g2=sys.argv[1:]
g0,g1,g2=(json.loads(x) for x in (g0,g1,g2))
t0,t1=g0["tuner"],g1["tuner"]
n=t1["collections"]-t0["collections"]
pause=(t1["pause_ns_total"]-t0["pause_ns_total"])/1e6
print(",".join(map(str,[V,S,R,ops,(int(c1)-int(c0))/100,n,round(pause,1),round(t1["pause_ns_max"]/1e6,2),
  int(hwm)//1024,g1["heap_size"]>>20,int(rssi)//1024,g2["heap_size"]>>20,t1["min_bytes_allocd"]>>10,t1["adjustments"]])))
PY
