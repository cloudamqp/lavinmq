#!/bin/bash
cd "$(dirname "$0")"
echo "variant,scenario,rep,ops,cpu_s,gcs,pause_ms,pause_max_ms,hwm_mb,heap_mb,rss_idle_mb,heap_idle_mb,floor_kb,adj" > res/matrix.csv
for r in 1 2 3; do for s in small large fanout conn queue; do for v in stock fsd1 fsd6 floor8m floor64m auto1; do
  ./run.sh $v $s $r >> res/matrix.csv || echo "$v,$s,$r,FAIL" >> res/matrix.csv
done; done; done
echo DONE
