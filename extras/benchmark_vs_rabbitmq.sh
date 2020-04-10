#!/bin/bash
set -eux

PERFTEST=../rabbitmq-perf-test-2.10.0/bin/runjava com.rabbitmq.perf.PerfTest

$PERFTEST -h $RABBITMQ -a --size 16 -z 30
$PERFTEST -h $AVALANCHEMQ -a --size 16 -z 30

$PERFTEST -h $RABBITMQ -a --size 4096 -z 30
$PERFTEST -h $AVALANCHEMQ -a --size 4096 -z 30

$PERFTEST -h $RABBITMQ --size 1024 -u q1 --flag persistent -ad false -z 30
$PERFTEST -h $AVALANCHEMQ --size 1024 -u q1 --flag persistent -ad false -z 30

$PERFTEST -h $RABBITMQ --size 1024 -u q1 --flag persistent -ad false -y 0 -C 1000000 -z 30
$PERFTEST -h $AVALANCHEMQ --size 1024 -u q1 --flag persistent -ad false -y 0 -C 1000000 -z 30

$PERFTEST -h $RABBITMQ --size 1024 -u q1 --flag persistent -ad false -x 0 -D 1000000 -z 30
$PERFTEST -h $AVALANCHEMQ --size 1024 -u q1 --flag persistent -ad false -x 0 -D 1000000 -z 30

$PERFTEST -h $RABBITMQ --size 1024 --queue-pattern 'perf-test-%d' --queue-pattern-from 1 --queue-pattern-to 1000 --producers 1500 --consumers 1000 --heartbeat-sender-threads 10 --publishing-interval 5

$PERFTEST -h $RABBITMQ --size 1024 --queue-pattern 'perf-test-%d' --queue-pattern-from 1 --queue-pattern-to 1000 --producers 3000 --consumers 1000 --heartbeat-sender-threads 10 --publishing-interval 5

$PERFTEST -h $AVALANCHEMQ --size 1024 --queue-pattern 'perf-test-%d' --queue-pattern-from 1 --queue-pattern-to 1000 --producers 3000 --consumers 1000 --heartbeat-sender-threads 10 --publishing-interval 5

$PERFTEST -h $AVALANCHEMQ --size 1024 --queue-pattern 'perf-test-%d' --queue-pattern-from 1 --queue-pattern-to 1000 --producers 9000 --consumers 1000 --heartbeat-sender-threads 10 --publishing-interval 5

#########################

# Publish to 100 queues, that all dead-letter into a single queue
$PERFTEST -h $RABBITMQ --size 1024 --queue-args x-message-ttl=10000,x-dead-letter-exchange=dlx -y 0 --flag persistent -ad false --queue-pattern 'q%d' --queue-pattern-from 1 --queue-pattern-to 10 --producers 10 --rate 1000
$PERFTEST -h $RABBITMQ --exchange dlx --type fanout -x 0 --queue dlq --flag persistent -ad false

# Restart a server with 2M msgs
$PERFTEST -h $RABBITMQ --size 0 -u q1 --flag persistent -ad false -y 0 -C 2000000
sudo service rabbitmq restart

$PERFTEST -h $AVALANCHEMQ --size 0 -u q1 --flag persistent -ad false -y 0 -C 2000000
sudo service avalanchemq restart

# Purge a queue with 2M msgs

# Do 2M bindings to a persistent queue

# Declare 100 000 queues

