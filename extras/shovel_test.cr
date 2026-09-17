# Black-box test suite for the Shovel feature, run against a *running* LavinMQ.
#
# Drives the server through its management API and AMQP port: creates a
# vhost, queues and shovels, serves the HTTP endpoint the HTTP scenarios
# deliver to, and checks what is observable from the outside — what ended up
# in which queue, what the endpoint received, and what the shovel reports.
#
# Usage:
#   make bin/shovel-test
#   bin/lavinmq --data-dir ./tmp/shovel-test &
#   bin/shovel-test [--host localhost] [--messages 10000] [--only overflow]
#
# Or against any broker, like the script it was converted from:
#   AMQP_URL=amqp://user:pass@host/vhost bin/shovel-test --http-port 15672
#
# Exit status is 0 when every scenario passed. Scenarios live in
# extras/shovel_test/scenarios/, one per delivery outcome or lifecycle path;
# the first one is the reproduction from
# https://github.com/cloudamqp/lavinmq/issues/1357.
#
# Vocabulary (Source, Destination, Outcome, queue-length run, ...) follows
# src/lavinmq/shovel/CONTEXT.md.

require "./shovel_test/suite"

ShovelTest::Suite.main
