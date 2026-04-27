# Benchmarking (lavinmqperf)

`lavinmqperf` is a built-in benchmarking tool for measuring LavinMQ performance.

## Usage

```
lavinmqperf [protocol] [scenario] [arguments]
```

Connect to a specific server:
```
lavinmqperf --uri=amqp://user:pass@host:5672 amqp throughput
```

## Protocols

- `amqp` — AMQP 0-9-1 benchmarks
- `mqtt` — MQTT benchmarks

## AMQP Scenarios

| Scenario | Description |
|----------|-------------|
| `throughput` | Measure message publish and consume throughput |
| `queue-count` | Measure performance with many queues |
| `queue-churn` | Measure queue creation and deletion rate |
| `connection-count` | Measure performance with many connections |
| `connection-churn` | Measure connection open/close rate |
| `channel-churn` | Measure channel open/close rate |
| `consumer-churn` | Measure consumer subscribe/cancel rate |
| `bind-churn` | Measure binding create/delete rate |

## MQTT Scenarios

| Scenario | Description |
|----------|-------------|
| `throughput` | Measure MQTT publish and subscribe throughput |

## Common Options

| Flag | Description |
|------|-------------|
| `--uri=URI` | AMQP or MQTT URI (default: `amqp://guest:guest@localhost` for AMQP scenarios, `mqtt://localhost:1883` for MQTT scenarios) |
| `-h`, `--help` | Show help |
| `-v`, `--version` | Show version |
