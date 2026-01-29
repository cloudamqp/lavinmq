module LavinMQ
  class Config
    annotation CliOpt; end
    annotation IniOpt; end
    annotation EnvOpt; end
    INI_SECTIONS = {"main", "amqp", "mqtt", "mgmt", "experimental", "clustering"}

    # Separate module for config option definitions. This keeps the option declarations
    # organized in one place, while config.cr contains the parsing and validation logic.
    # Config class includes this module to inherit all annotated properties.
    module Options
      DEFAULT_LOG_LEVEL     = ::Log::Severity::Info
      DEFAULT_PASSWORD_HASH = Auth::Password::SHA256Password.new("+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+") # Hash of 'guest'

      @[CliOpt("-c CONFIG", "--config=CONFIG", "Path to config file", section: "options")]
      @[EnvOpt("LAVINMQ_CONFIGURATION_DIRECTORY")]
      property config_file = ""

      @[CliOpt("-D DIRECTORY", "--data-dir=DIRECTORY", "Data directory", section: "options")]
      @[IniOpt(section: "main")]
      @[EnvOpt("LAVINMQ_DATADIR")]
      property data_dir : String = "/var/lib/lavinmq"

      @[CliOpt("-l LEVEL", "--log-level=LEVEL", "Log level (Default: info)", ->::Log::Severity.parse(String), section: "options")]
      @[IniOpt(section: "main", transform: ->::Log::Severity.parse(String))]
      property log_level : ::Log::Severity = DEFAULT_LOG_LEVEL

      @[CliOpt("-b BIND", "--bind=BIND", "IP address that both the AMQP and HTTP servers will listen on (default: 127.0.0.1)", ->parse_bind(String), section: "bindings")]
      property bind = "127.0.0.1"

      @[CliOpt("-p PORT", "--port=PORT", "AMQP port to listen on (default: 5672)", section: "bindings")]
      @[IniOpt(ini_name: port, section: "amqp")]
      @[EnvOpt("LAVINMQ_AMQP_PORT")]
      property amqp_port = 5672

      @[IniOpt(section: "main")]
      property log_file : String? = nil

      @[CliOpt("", "--pidfile=FILE", "Write the process ID to FILE on startup. The file is removed upon graceful shutdown.", section: "options")]
      @[IniOpt(section: "main")]
      property pidfile : String = ""

      @[CliOpt("", "--amqp-bind=BIND", "IP address that the AMQP server will listen on (default: 127.0.0.1)", section: "bindings")]
      @[IniOpt(ini_name: bind, section: "amqp")]
      @[EnvOpt("LAVINMQ_AMQP_BIND")]
      property amqp_bind = "127.0.0.1"

      @[CliOpt("", "--amqp-unix-path=PATH", "AMQP UNIX path to listen to", section: "bindings")]
      @[IniOpt(ini_name: unix_path, section: "amqp")]
      property unix_path : String = ""

      @[CliOpt("", "--amqps-port=PORT", "AMQPS port to listen on (default: -1)", section: "bindings")]
      @[IniOpt(ini_name: tls_port, section: "amqp")]
      @[EnvOpt("LAVINMQ_AMQPS_PORT")]
      property amqps_port = 5671

      @[IniOpt(ini_name: bind, section: "mqtt")]
      property mqtt_bind = "127.0.0.1"

      @[IniOpt(ini_name: port, section: "mqtt")]
      property mqtt_port = 1883

      @[IniOpt(ini_name: tls_port, section: "mqtt")]
      property mqtts_port = 8883

      @[CliOpt("", "--mqtt-unix-path=PATH", "MQTT UNIX path to listen to", section: "bindings")]
      @[IniOpt(ini_name: unix_path, section: "mqtt")]
      property mqtt_unix_path = ""

      @[IniOpt(section: "amqp", transform: ->(v : String) { true?(v) ? 1u8 : v.to_u8? || 0u8 })]
      property unix_proxy_protocol = 1_u8 # PROXY protocol version on unix domain socket connections

      @[IniOpt(section: "amqp", transform: ->(v : String) { true?(v) ? 1u8 : v.to_u8? || 0u8 })]
      property tcp_proxy_protocol = 0_u8 # PROXY protocol version on amqp tcp connections

      @[CliOpt("", "--http-bind=BIND", "IP address that the HTTP server will listen on (default: 127.0.0.1)", section: "bindings")]
      @[IniOpt(ini_name: bind, section: "mgmt")]
      @[EnvOpt("LAVINMQ_HTTP_BIND")]
      property http_bind = "127.0.0.1"

      @[CliOpt("", "--http-port=PORT", "HTTP port to listen on (default: 15672)", section: "bindings")]
      @[IniOpt(ini_name: port, section: "mgmt")]
      @[EnvOpt("LAVINMQ_HTTP_PORT")]
      property http_port = 15672

      @[CliOpt("", "--http-unix-path=PATH", "HTTP UNIX path to listen to", section: "bindings")]
      @[IniOpt(ini_name: unix_path, section: "mgmt")]
      property http_unix_path = ""

      @[CliOpt("", "--https-port=PORT", "HTTPS port to listen on (default: -1)", section: "bindings")]
      @[IniOpt(ini_name: tls_port, section: "mgmt")]
      @[EnvOpt("LAVINMQ_HTTPS_PORT")]
      property https_port = 15671

      @[CliOpt("", "--cert FILE", "TLS certificate (including chain)", section: "tls")]
      @[IniOpt(ini_name: tls_cert, section: "main")]
      @[EnvOpt("LAVINMQ_TLS_CERT_PATH")]
      property tls_cert_path = ""

      @[CliOpt("", "--ciphers CIPHERS", "List of TLS ciphers to allow", section: "tls")]
      @[IniOpt(section: "main")]
      @[EnvOpt("LAVINMQ_TLS_CIPHERS")]
      property tls_ciphers = ""

      @[CliOpt("", "--key FILE", "Private key for the TLS certificate", section: "tls")]
      @[IniOpt(ini_name: tls_key, section: "main")]
      @[EnvOpt("LAVINMQ_TLS_KEY_PATH")]
      property tls_key_path = ""

      @[CliOpt("", "--tls-min-version=VERSION", "Minimum allowed TLS version (default 1.2)", section: "tls")]
      @[IniOpt(section: "main")]
      @[EnvOpt("LAVINMQ_TLS_MIN_VERSION")]
      property tls_min_version = ""

      @[IniOpt(section: "main")]
      property tls_keylog_file = ""

      @[IniOpt(section: "main")]
      @[CliOpt("", "--metrics-http-bind=BIND", "IP address that the Prometheus server will bind to (default: 127.0.0.1)")]
      property metrics_http_bind = "127.0.0.1"

      @[IniOpt(section: "main")]
      @[CliOpt("", "--metrics-http-port=PORT", "HTTP port that prometheus will listen to (default: 15692)")]
      property metrics_http_port = 15692

      @[IniOpt(ini_name: permission_check_enabled, section: "mqtt")]
      property? mqtt_permission_check_enabled : Bool = false

      @[IniOpt(ini_name: on_leader_elected, section: "clustering")]
      @[CliOpt("", "--clustering-on-leader-elected=COMMAND", "Shell command to execute when elected leader", section: "clustering")]
      property clustering_on_leader_elected = "" # shell command to execute when elected leader

      @[IniOpt(ini_name: on_leader_lost, section: "clustering")]
      @[CliOpt("", "--clustering-on-leader-lost=COMMAND", "Shell command to execute when losing leadership", section: "clustering")]
      property clustering_on_leader_lost = "" # shell command to execute when losing leadership

      @[IniOpt(ini_name: max_packet_size, section: "mqtt")]
      property mqtt_max_packet_size = 268_435_455_u32 # bytes

      @[IniOpt(section: "mgmt")]
      property http_systemd_socket_name = "lavinmq-http.socket"

      @[IniOpt(section: "amqp")]
      property amqp_systemd_socket_name = "lavinmq-amqp.socket"

      @[IniOpt(section: "amqp")]
      property heartbeat = 300_u16 # second

      @[IniOpt(section: "amqp")]
      property frame_max = 131_072_u32 # bytes

      @[IniOpt(section: "amqp")]
      property channel_max = 2048_u16 # number

      @[IniOpt(section: "main")]
      property stats_interval = 5000 # millisecond

      @[IniOpt(section: "main")]
      property stats_log_size = 120 # 10 mins at 5s interval

      @[IniOpt(section: "main")]
      property? set_timestamp = false # in message headers when receive

      @[IniOpt(section: "main")]
      property socket_buffer_size = 16384 # bytes

      @[IniOpt(section: "main")]
      property? tcp_nodelay = false # bool

      @[IniOpt(section: "main")]
      property segment_size : Int32 = 8 * 1024**2 # bytes

      @[IniOpt(section: "mqtt")]
      property max_inflight_messages : UInt16 = UInt16::MAX # mqtt messages

      @[IniOpt(ini_name: default_vhost, section: "mqtt")]
      property default_mqtt_vhost = "/"

      @[IniOpt(section: "main", transform: ->tcp_keepalive?(String))]
      property tcp_keepalive : Tuple(Int32, Int32, Int32)? = {60, 10, 3} # idle, interval, probes/count

      @[IniOpt(section: "main")]
      property tcp_recv_buffer_size : Int32? = nil

      @[IniOpt(section: "main")]
      property tcp_send_buffer_size : Int32? = nil

      @[IniOpt(section: "amqp")]
      property max_message_size = 128 * 1024**2

      @[IniOpt(section: "main")]
      property? log_exchange : Bool = false

      @[IniOpt(section: "main")]
      property free_disk_min : Int64 = 0_i64 # bytes

      @[IniOpt(section: "main")]
      property free_disk_warn : Int64 = 0_i64 # bytes

      @[IniOpt(section: "main")]
      property max_deleted_definitions = 8192 # number of deleted queues, unbinds etc that compacts the definitions file

      @[IniOpt(section: "main")]
      property consumer_timeout : UInt64? = nil

      @[IniOpt(section: "main")]
      property consumer_timeout_loop_interval = 60 # seconds

      @[IniOpt(section: "experimental")]
      property yield_each_received_bytes = 131_072 # max number of bytes to read from a client connection without letting other tasks in the server do any work

      @[IniOpt(section: "experimental")]
      property yield_each_delivered_bytes = 1_048_576 # max number of bytes sent to a client without tending to other tasks in the server

      @[IniOpt(section: "main", transform: ->(s : String) { s.split(",").map(&.strip) })]
      property auth_backends : Array(String) = ["local"]

      @[CliOpt("", "--default-consumer-prefetch=NUMBER", "Default consumer prefetch (default 65535)", section: "options")]
      @[IniOpt(section: "main")]
      @[EnvOpt("LAVINMQ_DEFAULT_CONSUMER_PREFETCH")]
      property default_consumer_prefetch = UInt16::MAX

      @[CliOpt("", "--default-password=PASSWORD-HASH",
        "(Deprecated) Hashed password for default user (default: '+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+' (guest))",
        deprecated: "--default-password is deprecated, use --default-password-hash", section: "options")]
      @[IniOpt(section: "main", deprecated: "default_password_hash")]
      @default_password : Auth::Password::SHA256Password = DEFAULT_PASSWORD_HASH # Hashed password for default user
      def default_password=(value)
        # Forward value to the new property
        @default_password_hash = value
      end

      @[CliOpt("", "--default-password-hash=PASSWORD-HASH", "Hashed password for default user (default: '+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+' (guest))", section: "options")]
      @[IniOpt(section: "main")]
      @[EnvOpt("LAVINMQ_DEFAULT_PASSWORD")]
      property default_password_hash : Auth::Password::SHA256Password = DEFAULT_PASSWORD_HASH # Hashed password for default user

      @[CliOpt("", "--default-user=USER", "Default user (default: guest)", section: "options")]
      @[IniOpt(section: "main")]
      @[EnvOpt("LAVINMQ_DEFAULT_USER")]
      property default_user = "guest"

      @[CliOpt("", "--default-user-only-loopback=BOOL", "Limit guest user to only connect from loopback address", section: "options")]
      @[IniOpt(section: "main")]
      property? default_user_only_loopback : Bool = true

      @[CliOpt("", "--guest-only-loopback=BOOL", "Limit guest user to only connect from loopback address", deprecated: "Use --default-user-only-loopback instead.", section: "options")]
      @[IniOpt(section: "main", deprecated: "default_user_only_loopback")]
      property? guest_only_loopback : Bool = true

      @[CliOpt("", "--no-data-dir-lock", "Don't put a file lock in the data directory (default true)", section: "options")]
      @[IniOpt(section: "main")]
      property? data_dir_lock : Bool = true

      @[CliOpt("", "--raise-gc-warn", "Raise on GC warnings", section: "options")]
      property? raise_gc_warn : Bool = false

      @[CliOpt("", "--clustering", "Enable clustering", section: "clustering")]
      @[IniOpt(ini_name: enabled, section: "clustering")]
      @[EnvOpt("LAVINMQ_CLUSTERING")]
      property? clustering = false

      @[CliOpt("", "--clustering-advertised-uri=URI", "Advertised URI for the clustering server", section: "clustering")]
      @[IniOpt(ini_name: advertised_uri, section: "clustering")]
      @[EnvOpt("LAVINMQ_CLUSTERING_ADVERTISED_URI")]
      property clustering_advertised_uri : String? = nil

      @[CliOpt("", "--clustering-bind=BIND", "Listen for clustering followers on this address (default: localhost)", section: "clustering")]
      @[IniOpt(ini_name: bind, section: "clustering")]
      @[EnvOpt("LAVINMQ_CLUSTERING_BIND")]
      property clustering_bind = "127.0.0.1"

      @[CliOpt("", "--clustering-etcd-endpoints=URIs", "Comma separated host/port pairs (default: 127.0.0.1:2379)", section: "clustering")]
      @[IniOpt(ini_name: etcd_endpoints, section: "clustering")]
      @[EnvOpt("LAVINMQ_CLUSTERING_ETCD_ENDPOINTS")]
      property clustering_etcd_endpoints = "localhost:2379"

      @[CliOpt("", "--clustering-etcd-prefix=KEY", "Key prefix used in etcd (default: lavinmq)", section: "clustering")]
      @[IniOpt(ini_name: etcd_prefix, section: "clustering")]
      @[EnvOpt("LAVINMQ_CLUSTERING_ETCD_PREFIX")]
      property clustering_etcd_prefix = "lavinmq"

      @[CliOpt("", "--clustering-max-unsynced-actions=ACTIONS", "Maximum unsynced actions", section: "clustering")]
      @[IniOpt(ini_name: max_unsynced_actions, section: "clustering")]
      @[EnvOpt("LAVINMQ_CLUSTERING_MAX_UNSYNCED_ACTIONS")]
      property clustering_max_unsynced_actions = 8192 # number of unsynced clustering actions

      @[CliOpt("", "--clustering-port=PORT", "Listen for clustering followers on this port (default: 5679)", section: "clustering")]
      @[IniOpt(ini_name: port, section: "clustering")]
      @[EnvOpt("LAVINMQ_CLUSTERING_PORT")]
      property clustering_port = 5679

      @[IniOpt(section: "amqp")]
      property max_consumers_per_channel = 0

      # Deprecated options - these forward to the primary option in [main]

      @[IniOpt(ini_name: tls_cert, section: "amqp", deprecated: "tls_cert in [main]")]
      @amqp_tls_cert = ""

      def amqp_tls_cert=(value)
        @tls_cert_path = value
      end

      @[IniOpt(ini_name: tls_key, section: "amqp", deprecated: "tls_key in [main]")]
      @amqp_tls_key = ""

      def amqp_tls_key=(value)
        @tls_key_path = value
      end

      @[IniOpt(ini_name: tls_cert, section: "mgmt", deprecated: "tls_cert in [main]")]
      @mgmt_tls_cert = ""

      def mgmt_tls_cert=(value)
        @tls_cert_path = value
      end

      @[IniOpt(ini_name: tls_key, section: "mgmt", deprecated: "tls_key in [main]")]
      @mgmt_tls_key = ""

      def mgmt_tls_key=(value)
        @tls_key_path = value
      end

      @[IniOpt(ini_name: set_timestamp, section: "amqp", deprecated: "set_timestamp in [main]")]
      @amqp_set_timestamp = false

      def amqp_set_timestamp=(value)
        @set_timestamp = value
      end

      @[IniOpt(ini_name: consumer_timeout, section: "amqp", deprecated: "consumer_timeout in [main]")]
      @amqp_consumer_timeout : UInt64? = nil

      def amqp_consumer_timeout=(value)
        @consumer_timeout = value
      end

      @[IniOpt(ini_name: default_consumer_prefetch, section: "amqp", deprecated: "default_consumer_prefetch in [main]")]
      @amqp_default_consumer_prefetch = UInt16::MAX

      def amqp_default_consumer_prefetch=(value)
        @default_consumer_prefetch = value
      end
    end
  end
end
