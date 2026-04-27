# Configuration

LavinMQ can be configured through three methods, listed in order of precedence (highest first):

1. **CLI flags** — passed to the `lavinmq` binary
2. **Environment variables** — prefixed with `LAVINMQ_`
3. **INI configuration file** — specified with `-c` / `--config`
4. **Built-in defaults**

## INI File Format

The configuration file uses INI format with sections. Specify it with:

```
lavinmq --config /etc/lavinmq/lavinmq.ini
```

Or via the environment variable `LAVINMQ_CONFIGURATION_DIRECTORY`.

## [main] Section

| INI Key | CLI Flag | Env Var | Type | Default | Description |
|---------|----------|---------|------|---------|-------------|
| `data_dir` | `-D`, `--data-dir` | `LAVINMQ_DATADIR` | String | `/var/lib/lavinmq` | Data directory |
| `log_level` | `-l`, `--log-level` | — | String | `info` | Log level (debug, info, warn, error) |
| `log_file` | — | — | String | (none) | Log file path |
| `pidfile` | `--pidfile` | — | String | (empty) | PID file path |
| `tls_cert` | `--cert` | `LAVINMQ_TLS_CERT_PATH` | String | (empty) | TLS certificate path (including chain) |
| `tls_key` | `--key` | `LAVINMQ_TLS_KEY_PATH` | String | (empty) | TLS private key path |
| `tls_ciphers` | `--ciphers` | `LAVINMQ_TLS_CIPHERS` | String | (empty) | Allowed TLS ciphers |
| `tls_min_version` | `--tls-min-version` | `LAVINMQ_TLS_MIN_VERSION` | String | (empty) | Minimum TLS version (default 1.2) |
| `tls_keylog_file` | — | — | String | (empty) | TLS key log file (for debugging) |
| `tls_ktls` | `--tls-ktls` | — | Bool | `false` | Enable kernel TLS offloading |
| `stats_interval` | — | — | Int | `5000` | Statistics collection interval (ms) |
| `stats_log_size` | — | — | Int | `120` | Number of stats samples to retain |
| `set_timestamp` | — | — | Bool | `false` | Set timestamp on received messages |
| `socket_buffer_size` | — | — | Int | `16384` | Socket buffer size (bytes) |
| `tcp_nodelay` | — | — | Bool | `false` | Disable Nagle's algorithm |
| `tcp_keepalive` | — | — | String | `60,10,3` | TCP keepalive (idle, interval, probes) |
| `tcp_recv_buffer_size` | — | — | Int | (system) | TCP receive buffer size |
| `tcp_send_buffer_size` | — | — | Int | (system) | TCP send buffer size |
| `segment_size` | — | — | Int | `8388608` | Message store segment size (bytes, 8MB) |
| `free_disk_min` | — | — | Int | `0` | Minimum free disk space (bytes). Blocks publishing when exceeded. |
| `free_disk_warn` | — | — | Int | `0` | Free disk space warning threshold (bytes) |
| `max_deleted_definitions` | — | — | Int | `8192` | Deleted definitions before compaction |
| `consumer_timeout` | — | — | UInt64 | (none) | Consumer idle timeout (ms) |
| `consumer_timeout_loop_interval` | — | — | Int | `60` | Consumer timeout check interval (s) |
| `log_exchange` | — | — | Bool | `false` | Enable the log exchange |
| `auth_backends` | — | — | Array | `[]` | Authentication backends |
| `default_consumer_prefetch` | `--default-consumer-prefetch` | `LAVINMQ_DEFAULT_CONSUMER_PREFETCH` | UInt16 | `65535` | Default consumer prefetch |
| `default_user` | `--default-user` | `LAVINMQ_DEFAULT_USER` | String | `guest` | Default user name |
| `default_password_hash` | `--default-password-hash` | `LAVINMQ_DEFAULT_PASSWORD` | String | (guest hash) | Hashed password for default user |
| `default_user_only_loopback` | `--default-user-only-loopback` | — | Bool | `true` | Restrict default user to loopback |
| `data_dir_lock` | `--no-data-dir-lock` | — | Bool | `true` | File lock on data directory |
| `default_consistent_hash_algorithm` | — | — | String | `ring` | Consistent hash algorithm (ring, jump) |
| `metrics_http_bind` | `--metrics-http-bind` | — | String | `127.0.0.1` | Prometheus metrics bind address |
| `metrics_http_port` | `--metrics-http-port` | — | Int | `15692` | Prometheus metrics port |

## [amqp] Section

| INI Key | CLI Flag | Env Var | Type | Default | Description |
|---------|----------|---------|------|---------|-------------|
| `bind` | `--amqp-bind` | `LAVINMQ_AMQP_BIND` | String | `127.0.0.1` | AMQP bind address |
| `port` | `-p`, `--amqp-port` | `LAVINMQ_AMQP_PORT` | Int | `5672` | AMQP port |
| `tls_port` | `--amqps-port` | `LAVINMQ_AMQPS_PORT` | Int | `5671` | AMQPS port |
| `unix_path` | `--amqp-unix-path` | — | String | (empty) | AMQP Unix socket path |
| `unix_proxy_protocol` | — | — | UInt8 | `1` | PROXY protocol version on Unix sockets |
| `tcp_proxy_protocol` | — | — | UInt8 | `0` | PROXY protocol version on TCP |
| `heartbeat` | — | — | UInt16 | `300` | Heartbeat interval (seconds) |
| `frame_max` | — | — | UInt32 | `131072` | Maximum frame size (bytes) |
| `channel_max` | — | — | UInt16 | `2048` | Maximum channels per connection |
| `max_message_size` | — | — | Int | `134217728` | Maximum message size (bytes, 128MB) |
| `max_consumers_per_channel` | — | — | Int | `0` | Max consumers per channel (0 = unlimited) |
| `amqp_systemd_socket_name` | — | — | String | `lavinmq-amqp.socket` | SystemD socket name |

## [mqtt] Section

| INI Key | CLI Flag | Env Var | Type | Default | Description |
|---------|----------|---------|------|---------|-------------|
| `bind` | `--mqtt-bind` | — | String | `127.0.0.1` | MQTT bind address |
| `port` | `--mqtt-port` | — | Int | `1883` | MQTT port |
| `tls_port` | `--mqtts-port` | — | Int | `8883` | MQTTS port |
| `unix_path` | `--mqtt-unix-path` | — | String | (empty) | MQTT Unix socket path |
| `max_inflight_messages` | — | — | UInt16 | `65535` | Max unacknowledged messages per session |
| `max_packet_size` | — | — | UInt32 | `268435455` | Max MQTT packet size (bytes) |
| `default_vhost` | — | — | String | `/` | Default vhost for MQTT connections |
| `permission_check_enabled` | — | — | Bool | `false` | Enable MQTT permission checks |

## [mgmt] Section

| INI Key | CLI Flag | Env Var | Type | Default | Description |
|---------|----------|---------|------|---------|-------------|
| `bind` | `--http-bind` | `LAVINMQ_HTTP_BIND` | String | `127.0.0.1` | HTTP bind address |
| `port` | `--http-port` | `LAVINMQ_HTTP_PORT` | Int | `15672` | HTTP port |
| `tls_port` | `--https-port` | `LAVINMQ_HTTPS_PORT` | Int | `15671` | HTTPS port |
| `unix_path` | `--http-unix-path` | — | String | (empty) | HTTP Unix socket path |
| `http_systemd_socket_name` | — | — | String | `lavinmq-http.socket` | SystemD socket name |

## [clustering] Section

| INI Key | CLI Flag | Env Var | Type | Default | Description |
|---------|----------|---------|------|---------|-------------|
| `enabled` | `--clustering` | `LAVINMQ_CLUSTERING` | Bool | `false` | Enable clustering |
| `bind` | `--clustering-bind` | `LAVINMQ_CLUSTERING_BIND` | String | `127.0.0.1` | Clustering bind address |
| `port` | `--clustering-port` | `LAVINMQ_CLUSTERING_PORT` | Int | `5679` | Clustering port |
| `advertised_uri` | `--clustering-advertised-uri` | `LAVINMQ_CLUSTERING_ADVERTISED_URI` | String | (none) | Advertised URI for peers |
| `etcd_endpoints` | `--clustering-etcd-endpoints` | `LAVINMQ_CLUSTERING_ETCD_ENDPOINTS` | String | `localhost:2379` | etcd endpoints (comma-separated) |
| `etcd_prefix` | `--clustering-etcd-prefix` | `LAVINMQ_CLUSTERING_ETCD_PREFIX` | String | `lavinmq` | etcd key prefix |
| `max_unsynced_actions` | `--clustering-max-unsynced-actions` | `LAVINMQ_CLUSTERING_MAX_UNSYNCED_ACTIONS` | Int | `8192` | Max unsynced actions before sync |
| `on_leader_elected` | `--clustering-on-leader-elected` | — | String | (empty) | Shell command on leader election |
| `on_leader_lost` | `--clustering-on-leader-lost` | — | String | (empty) | Shell command on losing leadership |

## [oauth] Section

| INI Key | Type | Default | Description |
|---------|------|---------|-------------|
| `issuer` | URI | (none) | OAuth2/OIDC issuer URL |
| `resource_server_id` | String | (none) | Resource server identifier |
| `preferred_username_claims` | Array | `["sub", "client_id"]` | JWT claims for username extraction |
| `additional_scopes_keys` | Array | `[]` | Additional JWT claims to check for scopes |
| `scope_prefix` | String | (none) | Prefix to strip from scope strings |
| `verify_aud` | Bool | `true` | Verify JWT audience claim |
| `audience` | String | (none) | Expected JWT audience |
| `jwks_cache_ttl` | Duration | `1h` | JWKS cache TTL |

## [experimental] Section

| INI Key | Type | Default | Description |
|---------|------|---------|-------------|
| `yield_each_received_bytes` | Int | `131072` | Bytes received before yielding to other fibers |
| `yield_each_delivered_bytes` | Int | `1048576` | Bytes delivered before yielding to other fibers |

## [sni:hostname] Sections

Per-hostname TLS configuration. Create a section for each hostname:

```ini
[sni:example.com]
tls_cert = /path/to/example.com.crt
tls_key = /path/to/example.com.key
```

## Global CLI-Only Flags

| Flag | Description |
|------|-------------|
| `-b`, `--bind` | Bind address for both AMQP and HTTP |
| `-c`, `--config` | Path to config file |
| `--raise-gc-warn` | Raise on GC warnings |
| `--no-data-dir-lock` | Disable data directory file lock |

## Deprecated Options

| Deprecated | Replacement |
|-----------|-------------|
| `tls_cert` in `[amqp]` | `tls_cert` in `[main]` |
| `tls_key` in `[amqp]` | `tls_key` in `[main]` |
| `tls_cert` in `[mgmt]` | `tls_cert` in `[main]` |
| `tls_key` in `[mgmt]` | `tls_key` in `[main]` |
| `set_timestamp` in `[amqp]` | `set_timestamp` in `[main]` |
| `consumer_timeout` in `[amqp]` | `consumer_timeout` in `[main]` |
| `default_consumer_prefetch` in `[amqp]` | `default_consumer_prefetch` in `[main]` |
| `--default-password` | `--default-password-hash` |
| `--guest-only-loopback` | `--default-user-only-loopback` |

## Example Configuration

```ini
[main]
data_dir = /var/lib/lavinmq
log_level = info
tls_cert = /etc/lavinmq/cert.pem
tls_key = /etc/lavinmq/key.pem

[amqp]
bind = ::
port = 5672
tls_port = 5671

[mqtt]
bind = ::
port = 1883
tls_port = 8883

[mgmt]
bind = ::
port = 15672
tls_port = 15671

[clustering]
enabled = false
```
