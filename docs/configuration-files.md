
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is the LavinMQ configuration file?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>Most settings in LavinMQ can be tuned using the configuration file. The structure of the configuration file is <a href="https://en.wikipedia.org/wiki/INI_file" target="_blank">INI</a> and comprises key-value pairs for properties and sections that organize the properties. The sections are split into four; one [main] section for general settings, one for the LavinMQ Management Interface [mgmt], one for amqp settings [amqp], one for MQTT settings [mqtt], one for clustering settings [clustering]. The syntax can be explained as:
        <ul>
          <li>One setting uses one line</li>
          <li>Lines are structured Key = Value</li>
          <li>Any line starting with the ; character indicates a comment</li>
        </ul>
        </p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        Where is the configuration file located?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>The default configuration file location is <code class="language-plaintext">/etc/lavinmq/lavinmq.ini</code></p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect3" id="accordion3">
        How do config changes take effect?
      </button>
    </h3>
    <div id="sect3" class="accordion-content" role="region" aria-labelledby="accordion3">
      <p>Changes to the configuration file will be applied after a restart of LavinMQ.</p>
    </div>
  </div>
</div>

The tables below outline the different settings available for LavinMQ by section, including their default values and examples.

### [main]

<div class="bg-[#181818] border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
    <table class="divide-y divide-[#414040] conf-table">
      <tr>
        <td class="font-semibold">consumer_timeout</td>
        <td>
        Default consumer timeout for acks.
        <br/>
        <br/>
        Default:
        <code>consumer_timeout = nil (UInt64?)</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">data_dir</td>
        <td>
          The path of the data directory where all data is stored.
          <br/>
          <br/>
          Default: <code>data_dir = /var/lib/lavinmq (String)</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">data_dir_lock</td>
        <td>
          Use file lock in the data directory
          <br/>
          <br/>
          Default: <code>data_dir_lock = true</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">default_consumer_prefetch</td>
        <td>
        Default prefetch value for consumers if not set by consumer.
        <br/>
        <br/>
        Default:
        <code>default_consumer_prefetch = 65535</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">default_password</td>
        <td>
        Hashed password for default user. Use <code>lavinmqctl hash_password</code> or <code>/api/auth/hash_password</code> to generate password hash.
        <br/>
        <br/>
        Default:
        <code>default_password = +pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">default_user</td>
        <td>
        Default user.
        <br/>
        <br/>
        Default:
        <code>default_user = guest</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">default_user_only_loopback</td>
        <td>
        Limit default user to only connect from loopback address (127.0.0.1). Replaces the deprecated <code>guest_only_loopback</code> option.
        <br/>
        <br/>
        Default:
        <code>default_user_only_loopback = true</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">free_disk_min</td>
        <td>
          The minimum value of free disk space in bytes before LavinMQ starts to control flow.
          <br/>
          <br/>
          Default:
          <code>free_disk_min = 0</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">free_disk_warn</td>
        <td>
        The minimum value of free disk space in bytes before LavinMQ warns about low disk space.
        <br/>
        <br/>
        Default:
        <code>free_disk_warn = 0</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">guest_only_loopback</td>
        <td>
        <strong>DEPRECATED:</strong> This option is deprecated as of v2.2.0 and will be removed in the next major version. Use <code>default_user_only_loopback</code> instead, which has identical functionality but supports the new configurable default user feature.
        <br/>
        <br/>
        Limit guest user to only connect from loopback address.
        <br/>
        <br/>
        Default:
        <code>guest_only_loopback = true</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">log_exchange</td>
        <td>
          Enable log exchange
          <br/>
          <br/>
          Default: <code>log_exchange = false</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">log_file</td>
        <td>
          Path to file log file. If no path is set, log is written to STDOUT.
          <br/>
          <br/>
          Default: <code>log_file = nil</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">log_level</td>
        <td>
          Controls how detailed the log should be. The level can be one of:
          <ul>
            <li>
              none  (nothing is logged)
            </li>
            <li>
              fatal (only fatal errors are logged)
            </li>
            <li>
              error (fatal and errors are logged)
            </li>
            <li>
              warn (fatal, errors and warnings are logged)
            </li>
            <li>
              info (fatal, errors, warnings, and info messages are logged)
            </li>
            <li>
              debug (all messages are logged).
            </li>
          </ul>
          <br/>
          <br/>
          Default: <code>log_level = info</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">max_deleted_definitions</td>
        <td>
        Number of deleted queues, unbinds etc that compacts the definitions file.
        <br/>
        <br/>
        Default:
        <code>max_deleted_definitions = 8192</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">segment_size</td>
        <td>
        Size of segment files.
        <br/>
        <br/>
        Default:
        <code>segment_size = 8388608</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">set_timestamp</td>
        <td>
          Boolean value for setting the timestamp property.
          <br/>
          <br/>
          Default:
          <code>set_timestamp = false</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">socket_buffer_size</td>
        <td>
          Socket buffer size in bytes.
          <br/>
          <br/>
          Default:
          <code>socket_buffer_size = 16384</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">stats_interval</td>
        <td>
          Statistics collection interval in milliseconds.
          <br/>
          <br/>
          Default: <code>stats_interval = 5000</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">stats_log_size</td>
        <td>
          Number of entries in the statistics log file before oldest entry removed.
          <br/>
          <br/>
          Default:
          <code>stats_log_size = 120</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tcp_keepalive</td>
        <td>
          tcp_keepalive settings as a tuple, {idle, interval, probes/count}.
          <br/>
          <br/>
          Default:
          <code>tcp_keepalive = {60, 10, 3}</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tcp_nodelay</td>
        <td>
          Boolean value for disabling Nagle's algorithm, and sending the data as
          soon as it's available.
          <br/>
          <br/>
          Default:
          <code>tcp_nodelay = false</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tcp_recv_buffer_size</td>
        <td>
          TCP receive buffer size.
          <br/>
          <br/>
          Default:
          <code>tcp_recv_buffer_size = nil</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tcp_send_buffer_size</td>
        <td>
          TCP send buffer size.
          <br/>
          <br/>
          Default:
          <code>tcp_send_buffer_size = nil</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tls_cert</td>
        <td>
          TLS certificate (including chain).
          <br/>
          <br/>
          Default:
          <code>tls_cert = nil</code>
          <br/>
          Example:
          <code>tls_cert = /etc/lavinmq/cert.pem</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tls_key</td>
        <td>
          Private key for the TLS certificate.
          <br/>
          <br/>
          Default:
          <code>tls_key = nil</code>
          <br />
          Example:
          <code>ls_key = /etc/lavinmq/key.pem</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tls_ciphers</td>
        <td>
          List of TLS ciphers to allow
          <br/>
          <br/>
          Example:
          <code>tls_ciphers = nil</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tls_min_version</td>
        <td>
          Minimum allowed TLS version. Allowed options:
          <ul>
            <li>
              1.0
            </li>
            <li>
              1.1
            </li>
            <li>
              1.2
            </li>
            <li>
              1.3
            </li>
          </ul>
          <br/>
          Default: <code>tls_min_version = 1.2</code>
        </td>
      </tr>
    </table>
  </div>
</div>

### [mgmt]

<div class="bg-[#181818] border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
    <table class="divide-y divide-gray-300 conf-table">
      <tr>
        <td class="font-semibold">bind</td>
        <td>
          IP address that the HTTP server will listen on.
          <br/>
          <br/>
          Default:
          <code>bind = 127.0.0.1</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">port</td>
        <td>
          Port used for connections.
          <br/>
          <br/>
          Default:
          <code>port = 15672</code>
        </td>
      </tr>
      <!--<tr>
        <td class="font-bold">systemd_socket_name</td>
        <td>
          Default:
          <code>systemd_socket_name = lavinmq-http.socket</code>
        </td>
      </tr>-->
      <tr>
        <td class="font-semibold">tls_port</td>
        <td>
          Port used for TLS connections.
          <br/>
          <br/>
          Default:
          <br/>
          <code>tls_port = -1 (disabled)</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">unix_path</td>
        <td>
          UNIX path to listen to
          <br/>
          <br/>
          Default:
          <code>unix_path = /tmp/lavinmq-http.sock</code>
        </td>
      </tr>
    </table>
  </div>
</div>

### [amqp]

<div class="bg-[#181818] border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
    <table class="divide-y divide-gray-300 conf-table">
      <tr>
        <td class="font-semibold">bind</td>
        <td>
          IP address that both the AMQP and HTTP servers will listen on.
          <br/>
          <br/>
          Default:
          <code>bind = 127.0.0.1</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">channel_max</td>
        <td>
          Maximum number of channels to negotiate with clients. Setting to 0 means
          an unlimited number of channels.
          <br/>
          <br/>
          Default:
          <code>channel_max = 2048</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">frame_max</td>
        <td>
          Maximum frame size in bytes.
          <br/>
          <br/>
          Default:
          <code>frame_max = 1048576</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">heartbeat</td>
        <td>
          Timeout value in seconds suggested by the server during connection
          negotiation. If set to 0 on both server and client, heartbeats are
          disabled.
          <br/>
          <br/>
          Default:
          <code>Heartbeat = 0</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">max_message_size</td>
        <td>
        The maximum message size in bytes.
        <br/>
        <br/>
        Default: <code>max_message_size = 128 * 1024²</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">port</td>
        <td>
          Port used for AMQP connections
          <br/>
          <br/>
          Default:
          <code>port = 5672</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">systemd_socket_name</td>
        <td>
          Default:
          <code>systemd_socket_name = lavinmq-amqp.socket</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tcp_proxy_protocol</td>
        <td>
          Boolean value for PROXY protocol on amqp tcp connections
          <br/>
          <br/>
          Default:
          <code>tcp_proxy_protocol = false</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tls_port</td>
        <td>
          Port used for TLS (AMQPS) connections
          <br/>
          <br/>
          Default:
          <code>tls_port = -1 (disabled)</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">unix_path</td>
        <td>
          UNIX path to listen to
          <br/>
          <br/>
          Default:
          <code>unix_path = /tmp/lavinmq.sock</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">unix_proxy_protocol</td>
        <td>
          Boolean value for PROXY protocol on unix domain socket connections
          <br/>
          <br/>
          Default:
          <code>unix_proxy_protocol = true</code>
        </td>
      </tr>
      <tr>
        <td class="font-bold">max_consumers_per_channel</td>
        <td>
          Maximum number of consumers allowed per AMQP channel. For unlimited consumers per channel set this value to 0.
          <br/>
          <br/>
          Default:
          <code>max_consumers_per_channel = 0</code>
        </td>
      </tr>
    </table>
  </div>
</div>

### [mqtt]

<div class="bg-[#181818] border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
    <table class="divide-y divide-gray-300 conf-table">
      <tr>
        <td class="font-semibold">bind</td>
        <td>
          IP address that AMQP, MQTT, and HTTP servers will listen on.
          <br/>
          <br/>
          Default:
          <code>bind = 127.0.0.1</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">port</td>
        <td>
          Port for non-TLS MQTT connections.
          <br/>
          <br/>
          Default:
          <code>port = 1883</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">tls_port</td>
        <td>
          Port for TLS-enabled MQTT connections
          <br/>
          <br/>
          Default:
          <br/>
          <code>tls-port = -1 (disabled)</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">unix_path</td>
        <td>
          UNIX path to listen to
          <br/>
          <br/>
          Default:
          <code>unix_path = /tmp/lavinmq-mqtt.sock</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">max_inflight_messages</td>
        <td>
          Maximum number of messages in flight simultaneously.
          <br/>
          <br/>
          Default:
          <code>65535</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">permission_check_enabled</td>
        <td>
          Enable permission checks for MQTT publish and subscribe operations. When enabled, publishing (including will messages) requires write permission on the MQTT exchange, and subscribing requires read permission on the MQTT exchange or write permission on the client's session queue.
          <br/>
          <br/>
          Available since v2.5.0.
          <br/>
          <br/>
          Default:
          <code>permission_check_enabled = false</code>
        </td>
      </tr>
    </table>
  </div>
</div>

### [clustering]

<div class="bg-[#181818] border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
    <table class="divide-y divide-gray-300 conf-table">
      <tr>
        <td class="font-semibold">advertised_uri</td>
        <td>
        Advertised URI for the clustering server.
        <br/>
        <br/>
        Default: <code>advertised_uri = nil</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">bind</td>
        <td>
        Listen for clustering followers on this address.
        <br/>
        <br/>
        Default: <code>bind = 127.0.0.1</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">enabled</td>
        <td>
        Enable clustering.
        <br/>
        <br/>
        Default: <code>enabled = false</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">etcd_endpoints</td>
        <td>
        Comma separated host:port pairs
        <br/>
        <br/>
        Default: <code>etcd_endpoints = localhost:2379</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">etcd_prefix</td>
        <td>
        Key prefix used in etcd.
        <br/>
        <br/>
        Default: <code>etcd_prefix = lavinmq</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">max_unsynced_actions</td>
        <td>
        Maximum number of unsynced actions.
        <br/>
        <br/>
        Default: <code>max_unsynced_actions = 8192</code>
        </td>
      </tr>
      <tr>
        <td class="font-semibold">port</td>
        <td>
        Listen for clustering followers on this port.
        <br/>
        <br/>
        Default: <code>port = 5679</code>
        </td>
      </tr>
    </table>
  </div>
</div>

## Example configuration file

An example configuration file can be found in the LavinMQ repo on [GitHub.](https://github.com/cloudamqp/lavinmq/blob/main/extras/lavinmq.ini)

```bash
[main]
data_dir = /var/lib/lavinmq
default_user_only_loopback = true
log_level = info

[mgmt]
bind = 0.0.0.0
port = 15672
tls_port = 15671
unix_path = /tmp/lavinmq-http.sock

[amqp]
bind = 0.0.0.0
port = 5672
tcp_proxy_protocol = false
tls_port = 5671
unix_path = /tmp/lavinmq.sock
unix_proxy_protocol = true
```
