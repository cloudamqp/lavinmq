#!/usr/bin/bash
set -eux

bin/lavinmq -D /tmp/amqp --amqp-unix-path /tmp/lavinmq_amqp.sock &
LAVINPID=$!

while ! test -S /tmp/lavinmq_amqp.sock ; do sleep 1; done
sleep 1

bin/lavinmqperf throughput -z 60 > $1

kill -INT $LAVINPID
wait $LAVINPID
