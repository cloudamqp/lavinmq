#!/bin/bash
cd "$(dirname "$0")"
echo "variant,scenario,rep,ops,cpu_s,gcs,pause_ms,pause_max_ms,hwm_mb,heap_mb,rss_idle_mb,heap_idle_mb,floor_kb,adj" > res/big.csv
for r in 1 2; do for s in conn small queue; do for v in stock fsd1 fsd6 floor64m auto1; do
  BIG=1 timeout 300 ./run.sh $v $s $r >> res/big.csv || echo "$v,big-$s,$r,FAIL" >> res/big.csv
  pkill -f "bin/lavinmq " ; pkill -f lavinmqperf; pkill -f "sleep 3600"; sleep 1
done; done; done
echo DONE
