#!/bin/bash
cd "$(dirname "$0")"
echo "cpuset,markers,load,rep,n,avg_ms,max_ms,heap_mb,markers_m1" > res/markers.csv
for r in 1 2 3; do for l in idle busy; do for cs in 0,1 0-3; do for m in def 1 2 4; do
  timeout 300 ./markers.sh $cs $m $l $r >> res/markers.csv || echo "$cs,$m,$l,$r,FAIL" >> res/markers.csv
  pkill -f "bin/lavinmq "; pkill -f lavinmqperf; pkill -f "sleep 3600"; sleep 1
done; done; done; done
echo DONE
