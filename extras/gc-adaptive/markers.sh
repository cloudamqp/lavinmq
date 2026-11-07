#!/bin/bash
# markers.sh CPUSET MARKERS LOAD(idle|busy) REP -> CSV: cpuset,markers,load,rep,n,avg_ms,max_ms,heap_mb
set -u
CS=$1; M=$2; L=$3; R=$4
SP=${SP:-/tmp/gc-adaptive}
cd /home/user/lavinmq
DATA=$SP/data; rm -rf $DATA; mkdir -p $DATA
envs=(GC_UNMAP_THRESHOLD=1); [ "$M" != def ] && envs+=(GC_MARKERS=$M)
env "${envs[@]}" taskset -c $CS bin/lavinmq --data-dir $DATA --log-level=warn > $SP/res/mk.log 2>&1 &
BP=$!
until curl -fs -u guest:guest localhost:15672/api/overview >/dev/null 2>&1; do sleep 0.2; done
P0="taskset -c 2,3 bin/lavinmqperf"
sleep 3600 | $P0 queue-count -q 10000 -n > $SP/res/qc.log 2>&1 &
until grep -q "10000 queues" $SP/res/qc.log; do sleep 0.5; done
sleep 3600 | $P0 connection-count -x 2000 -C 1 > $SP/res/cc.log 2>&1 &
until grep -q "2000 connections" $SP/res/cc.log; do sleep 0.5; done
pkill -f "lavinmqperf queue-count"
[ "$L" = busy ] && { $P0 throughput -x 1 -y 1 -s 256 -z 60 -q > /dev/null 2>&1 & sleep 3; }
gc() { curl -fs -u guest:guest localhost:15672/api/nodes/gc_stats; }
g0=$(gc)
for i in $(seq 10); do curl -fs -XPOST -u guest:guest localhost:15672/api/nodes/gc_collect; sleep 0.5; done
g1=$(gc)
pkill -f "lavinmqperf"; pkill -f "sleep 3600"
kill $BP; wait $BP 2>/dev/null
python3 -I - "$CS" "$M" "$L" "$R" "$g0" "$g1" <<'PY'
import sys, json
cs,m,l,r,g0,g1=sys.argv[1:]
a,b=json.loads(g0),json.loads(g1)
n=b["tuner"]["collections"]-a["tuner"]["collections"]
p=(b["tuner"]["pause_ns_total"]-a["tuner"]["pause_ns_total"])/1e6
print(f'{cs.replace(",","+")},{m},{l},{r},{n},{p/n:.1f},{b["tuner"]["pause_ns_max"]/1e6:.1f},{b["heap_size"]>>20},{b["markers_m1"]}')
PY
