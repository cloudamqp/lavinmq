require "log/spec"
require "./spec_helper"
require "../src/lavinmq/config"
require "../src/lavinmq/launcher"
require "../src/stdlib/openssl_on_server_name"

# Test-only seam: drive the real SIGHUP reload path without raising the signal.
# A reopened method can call the private `reload_server` because it's in the class.
class LavinMQ::Launcher
  def reload!
    reload_server
  end
end

describe LavinMQ::Config do
  it "should remember the config file path" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        log_level = fatal
        data_dir = /tmp/lavinmq-spec
        [mgmt]
        [amqp]
      CONFIG
    end
    config = LavinMQ::Config.new
    argv = ["-c", config_file.path]
    config.parse(argv)
    config.config_file.should eq config_file.path
    config.data_dir.should eq "/tmp/lavinmq-spec"
    config.log_level.to_s.should eq "Fatal"
  end

  it "defaults the control socket path to /tmp/lavinmqctl.sock" do
    LavinMQ::Config.new.control_unix_path.should eq "/tmp/lavinmqctl.sock"
  end

  it "should prioritize CLI arguments over other arguments" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        log_level = fatal
        data_dir = /tmp/lavinmq-ini
      CONFIG
    end
    ENV["LAVINMQ_DATADIR"] = "/tmp/lavinmq-env"
    config = LavinMQ::Config.new
    argv = ["-c", config_file.path, "-D", "/tmp/lavinmq-cli"]
    config.parse(argv)
    config.data_dir.should eq "/tmp/lavinmq-cli"
  ensure
    ENV.delete("LAVINMQ_DATADIR")
  end

  it "should prioritize ENV arguments over INI arguments" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        data_dir = /tmp/lavinmq-ini
      CONFIG
    end
    ENV["LAVINMQ_DATADIR"] = "/tmp/lavinmq-env"
    config = LavinMQ::Config.new
    argv = ["-c", config_file.path]
    config.parse(argv)
    config.data_dir.should eq "/tmp/lavinmq-env"
  ensure
    ENV.delete("LAVINMQ_DATADIR")
  end

  it "should reload config with new log level" do
    config_file = File.tempfile("lavinmq-config", ".ini")
    begin
      File.write(config_file.path, <<-CONFIG
        [main]
        log_level = info
      CONFIG
      )
      config = LavinMQ::Config.new
      argv = ["-c", config_file.path]
      config.parse(argv)
      config.log_level.should eq ::Log::Severity::Info

      # Update config file with new log level
      File.write(config_file.path, <<-CONFIG
        [main]
        log_level = debug
      CONFIG
      )
      config.reload
      config.log_level.should eq ::Log::Severity::Debug
    ensure
      File.delete(config_file.path) if File.exists?(config_file.path)
      # Reset log level to default for other specs
      Log.setup(:fatal)
    end
  end

  it "Can parse all INI arguments" do
    begin
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [main]
          data_dir = /tmp/lavinmq-test
          log_level = debug
          log_file = /tmp/lavinmq-test.log
          stats_interval = 10000
          stats_log_size = 240
          set_timestamp = true
          socket_buffer_size = 32768
          tcp_nodelay = true
          segment_size = 16777216
          sync = false
          tcp_keepalive = 120:20:5
          tcp_recv_buffer_size = 65536
          tcp_send_buffer_size = 65536
          log_exchange = true
          free_disk_min = 1073741824
          free_disk_warn = 5368709120
          max_deleted_definitions = 16384
          consumer_timeout = 3600
          consumer_timeout_loop_interval = 120
          auth_backends = ldap,basic
          default_consumer_prefetch = 1000
          default_password_hash = +pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+
          default_user = admin
          default_user_only_loopback = false
          data_dir_lock = false
          tls_cert = /etc/lavinmq/cert.pem
          tls_ciphers = ECDHE-RSA-AES256-GCM-SHA384
          tls_key = /etc/lavinmq/key.pem
          tls_min_version = 1.3
          tls_keylog_file = /tmp/keylog.txt
          metrics_http_bind = 0.0.0.0
          metrics_http_port = 9090
          control_unix_path = /tmp/lavinmqctl-ini.sock

          [amqp]
          bind = 0.0.0.0
          port = 5673
          tls_port = 5674
          unix_path = /tmp/lavinmq.sock
          tcp_proxy_protocol = 1
          proxy_protocol_trusted_sources = 10.0.0.1
          heartbeat = 600
          frame_max = 262144
          channel_max = 4096
          max_message_size = 268435456
          amqp_systemd_socket_name = custom-amqp.socket

          [mqtt]
          bind = 0.0.0.0
          port = 1884
          tls_port = 8884
          unix_path = /tmp/mqtt.sock
          permission_check_enabled = true
          max_packet_size = 536870910
          max_inflight_messages = 100
          default_vhost = /mqtt
          client_id_validation = username

          [mgmt]
          bind = 0.0.0.0
          port = 15673
          tls_port = 15674
          unix_path = /tmp/mgmt.sock
          http_systemd_socket_name = custom-http.socket

          [experimental]
          yield_each_received_bytes = 262144
          yield_each_delivered_bytes = 2097152

          [clustering]
          enabled = true
          bind = 0.0.0.0
          port = 5680
          etcd_endpoints = localhost:2380,localhost:2381
          etcd_prefix = test-lavinmq
          max_unsynced_actions = 16384
          advertised_uri = lavinmq://localhost:5680
          on_leader_elected = echo "Leader elected"
          on_leader_lost = echo "Leader lost"
        CONFIG
      end
      config = LavinMQ::Config.new
      argv = ["-c", config_file.path]
      config.parse(argv)

      # Main section
      config.data_dir.should eq "/tmp/lavinmq-test"
      config.log_level.should eq ::Log::Severity::Debug
      config.log_file.should eq "/tmp/lavinmq-test.log"
      config.stats_interval.should eq 10000
      config.stats_log_size.should eq 240
      config.set_timestamp?.should be_true
      config.socket_buffer_size.should eq 32768
      config.tcp_nodelay?.should be_true
      config.segment_size.should eq 16777216
      config.sync?.should be_false
      config.tcp_keepalive.should eq({120, 20, 5})
      config.tcp_recv_buffer_size.should eq 65536
      config.tcp_send_buffer_size.should eq 65536
      config.log_exchange?.should be_true
      config.free_disk_min.should eq 1073741824
      config.free_disk_warn.should eq 5368709120
      config.max_deleted_definitions.should eq 16384
      config.consumer_timeout.should eq 3600
      config.consumer_timeout_loop_interval.should eq 120
      config.auth_backends.should eq ["ldap", "basic"]
      config.default_consumer_prefetch.should eq 1000
      config.default_user.should eq "admin"
      config.default_user_only_loopback?.should be_false
      config.data_dir_lock?.should be_false
      config.tls_cert_path.should eq "/etc/lavinmq/cert.pem"
      config.tls_ciphers.should eq "ECDHE-RSA-AES256-GCM-SHA384"
      config.tls_key_path.should eq "/etc/lavinmq/key.pem"
      config.tls_min_version.should eq "1.3"
      config.tls_keylog_file.should eq "/tmp/keylog.txt"
      config.metrics_http_bind.should eq "0.0.0.0"
      config.metrics_http_port.should eq 9090
      config.control_unix_path.should eq "/tmp/lavinmqctl-ini.sock"

      # AMQP section
      config.amqp_bind.should eq "0.0.0.0"
      config.amqp_port.should eq 5673
      config.amqps_port.should eq 5674
      config.unix_path.should eq "/tmp/lavinmq.sock"
      config.tcp_proxy_protocol?.should be_true
      config.proxy_protocol_trusted_sources[0].matches?("10.0.0.1").should be_true
      config.heartbeat.should eq 600
      config.frame_max.should eq 262144
      config.channel_max.should eq 4096
      config.max_message_size.should eq 268435456
      config.amqp_systemd_socket_name.should eq "custom-amqp.socket"

      # MQTT section
      config.mqtt_bind.should eq "0.0.0.0"
      config.mqtt_port.should eq 1884
      config.mqtts_port.should eq 8884
      config.mqtt_unix_path.should eq "/tmp/mqtt.sock"
      config.mqtt_permission_check_enabled?.should be_true
      config.mqtt_max_packet_size.should eq 536870910
      config.max_inflight_messages.should eq 100
      config.default_mqtt_vhost.should eq "/mqtt"
      config.mqtt_client_id_validation.should eq LavinMQ::MQTT::ClientIdValidation::Username

      # MGMT section
      config.http_bind.should eq "0.0.0.0"
      config.http_port.should eq 15673
      config.https_port.should eq 15674
      config.http_unix_path.should eq "/tmp/mgmt.sock"
      config.http_systemd_socket_name.should eq "custom-http.socket"

      # Experimental section
      config.yield_each_received_bytes.should eq 262144
      config.yield_each_delivered_bytes.should eq 2097152

      # Clustering section
      config.clustering?.should be_true
      config.clustering_bind.should eq "0.0.0.0"
      config.clustering_port.should eq 5680
      config.clustering_etcd_endpoints.should eq "localhost:2380,localhost:2381"
      config.clustering_etcd_prefix.should eq "test-lavinmq"
      config.clustering_advertised_uri.should eq "lavinmq://localhost:5680"
      config.clustering_on_leader_elected.should eq "echo \"Leader elected\""
      config.clustering_on_leader_lost.should eq "echo \"Leader lost\""
    ensure
      # Reset log level to default for other specs
      Log.setup(:fatal)
    end
  end

  it "can parse all CLI argumetns" do
    config = LavinMQ::Config.new
    argv = [
      "-D", "/tmp/lavinmq-cli",
      "-l", "warn",
      "-b", "192.168.1.1",
      "-p", "5673",
      "--amqp-bind=10.0.0.1",
      "--amqp-unix-path=/tmp/amqp.sock",
      "--amqps-port=5675",
      "--http-bind=172.16.0.1",
      "--http-port=15673",
      "--http-unix-path=/tmp/http.sock",
      "--control-unix-path=/tmp/lavinmqctl-cli.sock",
      "--mqtt-bind=10.0.0.2",
      "--mqtt-port=1884",
      "--mqtts-port=8884",
      "--mqtt-unix-path=/tmp/mqtt.sock",
      "--https-port=15675",
      "--cert=/etc/ssl/cert.pem",
      "--ciphers=ECDHE-RSA-AES256-GCM-SHA384",
      "--key=/etc/ssl/key.pem",
      "--tls-min-version=1.3",
      "--metrics-http-bind=0.0.0.0",
      "--metrics-http-port=9091",
      "--clustering-on-leader-elected=echo leader",
      "--clustering-on-leader-lost=echo lost",
      "--default-consumer-prefetch=500",
      "--default-user=cliuser",
      "--default-user-only-loopback=false",
      "--no-data-dir-lock",
      "--no-sync",
      "--clustering",
      "--raise-gc-warn",
      "--clustering-advertised-uri=lavinmq://test:5679",
      "--clustering-bind=0.0.0.0",
      "--clustering-etcd-endpoints=etcd1:2379,etcd2:2379",
      "--clustering-etcd-prefix=cli-prefix",
      "--clustering-max-unsynced-actions=4096",
      "--clustering-port=5680",
    ]
    config.parse(argv)

    config.data_dir.should eq "/tmp/lavinmq-cli"
    config.log_level.should eq ::Log::Severity::Warn
    config.bind.should eq "192.168.1.1"
    config.amqp_port.should eq 5673
    config.amqp_bind.should eq "10.0.0.1"
    config.unix_path.should eq "/tmp/amqp.sock"
    config.amqps_port.should eq 5675
    config.http_bind.should eq "172.16.0.1"
    config.http_port.should eq 15673
    config.http_unix_path.should eq "/tmp/http.sock"
    config.control_unix_path.should eq "/tmp/lavinmqctl-cli.sock"
    config.mqtt_bind.should eq "10.0.0.2"
    config.mqtt_port.should eq 1884
    config.mqtts_port.should eq 8884
    config.mqtt_unix_path.should eq "/tmp/mqtt.sock"
    config.https_port.should eq 15675
    config.tls_cert_path.should eq "/etc/ssl/cert.pem"
    config.tls_ciphers.should eq "ECDHE-RSA-AES256-GCM-SHA384"
    config.tls_key_path.should eq "/etc/ssl/key.pem"
    config.tls_min_version.should eq "1.3"
    config.metrics_http_bind.should eq "0.0.0.0"
    config.metrics_http_port.should eq 9091
    config.clustering_on_leader_elected.should eq "echo leader"
    config.clustering_on_leader_lost.should eq "echo lost"
    config.default_consumer_prefetch.should eq 500
    config.default_user.should eq "cliuser"
    config.default_user_only_loopback?.should be_false
    config.data_dir_lock?.should be_false
    config.sync?.should be_false
    config.clustering?.should be_true
    config.raise_gc_warn?.should be_true
    config.clustering_advertised_uri.should eq "lavinmq://test:5679"
    config.clustering_bind.should eq "0.0.0.0"
    config.clustering_etcd_endpoints.should eq "etcd1:2379,etcd2:2379"
    config.clustering_etcd_prefix.should eq "cli-prefix"
    config.clustering_port.should eq 5680
  end

  it "can parse -d/--debug flag for verbose logging" do
    config = LavinMQ::Config.new
    config.parse(["-d"])
    config.log_level.should eq ::Log::Severity::Debug

    config2 = LavinMQ::Config.new
    config2.parse(["--debug"])
    config2.log_level.should eq ::Log::Severity::Debug
  end

  it "can parse all ENV arguments" do
    begin
      ENV["LAVINMQ_CONFIGURATION_DIRECTORY"] = "/etc/custom"
      ENV["LAVINMQ_DATADIR"] = "/tmp/lavinmq-env"
      ENV["LAVINMQ_AMQP_PORT"] = "5674"
      ENV["LAVINMQ_AMQP_BIND"] = "10.1.1.1"
      ENV["LAVINMQ_AMQPS_PORT"] = "5676"
      ENV["LAVINMQ_HTTP_BIND"] = "10.2.2.2"
      ENV["LAVINMQ_HTTP_PORT"] = "15674"
      ENV["LAVINMQ_HTTPS_PORT"] = "15676"
      ENV["LAVINMQ_TLS_CERT_PATH"] = "/etc/certs/env-cert.pem"
      ENV["LAVINMQ_TLS_CIPHERS"] = "ENV-CIPHER-SUITE"
      ENV["LAVINMQ_TLS_KEY_PATH"] = "/etc/certs/env-key.pem"
      ENV["LAVINMQ_TLS_MIN_VERSION"] = "1.2"
      ENV["LAVINMQ_DEFAULT_CONSUMER_PREFETCH"] = "2000"
      ENV["LAVINMQ_DEFAULT_USER"] = "envuser"
      ENV["LAVINMQ_CLUSTERING"] = "true"
      ENV["LAVINMQ_CLUSTERING_ADVERTISED_URI"] = "lavinmq://env:5679"
      ENV["LAVINMQ_CLUSTERING_BIND"] = "10.3.3.3"
      ENV["LAVINMQ_CLUSTERING_ETCD_ENDPOINTS"] = "env-etcd:2379"
      ENV["LAVINMQ_CLUSTERING_ETCD_PREFIX"] = "env-prefix"
      ENV["LAVINMQ_CLUSTERING_MAX_UNSYNCED_ACTIONS"] = "2048"
      ENV["LAVINMQ_CLUSTERING_PORT"] = "5681"
      ENV["LAVINMQ_SYNC"] = "false"
      ENV["LAVINMQ_CONTROL_UNIX_PATH"] = "/tmp/lavinmqctl-env.sock"
      config = LavinMQ::Config.new
      config.parse([] of String)

      config.data_dir.should eq "/tmp/lavinmq-env"
      config.amqp_port.should eq 5674
      config.amqp_bind.should eq "10.1.1.1"
      config.amqps_port.should eq 5676
      config.http_bind.should eq "10.2.2.2"
      config.http_port.should eq 15674
      config.https_port.should eq 15676
      config.tls_cert_path.should eq "/etc/certs/env-cert.pem"
      config.tls_ciphers.should eq "ENV-CIPHER-SUITE"
      config.tls_key_path.should eq "/etc/certs/env-key.pem"
      config.tls_min_version.should eq "1.2"
      config.default_consumer_prefetch.should eq 2000
      config.default_user.should eq "envuser"
      config.clustering?.should be_true
      config.clustering_advertised_uri.should eq "lavinmq://env:5679"
      config.clustering_bind.should eq "10.3.3.3"
      config.clustering_etcd_endpoints.should eq "env-etcd:2379"
      config.clustering_etcd_prefix.should eq "env-prefix"
      config.clustering_port.should eq 5681
      config.control_unix_path.should eq "/tmp/lavinmqctl-env.sock"
    ensure
      ENV.delete("LAVINMQ_CONFIGURATION_DIRECTORY")
      ENV.delete("LAVINMQ_DATADIR")
      ENV.delete("LAVINMQ_AMQP_PORT")
      ENV.delete("LAVINMQ_AMQP_BIND")
      ENV.delete("LAVINMQ_AMQPS_PORT")
      ENV.delete("LAVINMQ_HTTP_BIND")
      ENV.delete("LAVINMQ_HTTP_PORT")
      ENV.delete("LAVINMQ_HTTPS_PORT")
      ENV.delete("LAVINMQ_TLS_CERT_PATH")
      ENV.delete("LAVINMQ_TLS_CIPHERS")
      ENV.delete("LAVINMQ_TLS_KEY_PATH")
      ENV.delete("LAVINMQ_TLS_MIN_VERSION")
      ENV.delete("LAVINMQ_DEFAULT_CONSUMER_PREFETCH")
      ENV.delete("LAVINMQ_DEFAULT_USER")
      ENV.delete("LAVINMQ_CLUSTERING")
      ENV.delete("LAVINMQ_CLUSTERING_ADVERTISED_URI")
      ENV.delete("LAVINMQ_CLUSTERING_BIND")
      ENV.delete("LAVINMQ_CLUSTERING_ETCD_ENDPOINTS")
      ENV.delete("LAVINMQ_CLUSTERING_ETCD_PREFIX")
      ENV.delete("LAVINMQ_CLUSTERING_MAX_UNSYNCED_ACTIONS")
      ENV.delete("LAVINMQ_CLUSTERING_PORT")
      ENV.delete("LAVINMQ_CONTROL_UNIX_PATH")
    end
  end

  it "uses systemd STATE_DIRECTORY as default for data_dir" do
    begin
      ENV["STATE_DIRECTORY"] = "/var/lib/custom-state"
      config = LavinMQ::Config.new
      config.parse([] of String)
      config.data_dir.should eq "/var/lib/custom-state"
    ensure
      ENV.delete("STATE_DIRECTORY")
    end
  end

  it "STATE_DIRECTORY takes precedence over INI data_dir" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        data_dir = /tmp/lavinmq-ini
      CONFIG
    end
    begin
      ENV["STATE_DIRECTORY"] = "/var/lib/custom-state"
      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.data_dir.should eq "/var/lib/custom-state"
    ensure
      ENV.delete("STATE_DIRECTORY")
    end
  end

  it "LAVINMQ_DATADIR takes precedence over STATE_DIRECTORY" do
    begin
      ENV["STATE_DIRECTORY"] = "/var/lib/custom-state"
      ENV["LAVINMQ_DATADIR"] = "/var/lib/lavinmq-explicit"
      config = LavinMQ::Config.new
      config.parse([] of String)
      config.data_dir.should eq "/var/lib/lavinmq-explicit"
    ensure
      ENV.delete("STATE_DIRECTORY")
      ENV.delete("LAVINMQ_DATADIR")
    end
  end

  it "uses systemd CONFIGURATION_DIRECTORY for config file lookup" do
    config_dir = File.tempname("lavinmq-conf")
    Dir.mkdir_p(config_dir)
    File.write(File.join(config_dir, "lavinmq.ini"), "[main]\nlog_level = fatal\n")
    begin
      ENV["CONFIGURATION_DIRECTORY"] = config_dir
      config = LavinMQ::Config.new
      config.parse([] of String)
      config.config_file.should eq File.join(config_dir, "lavinmq.ini")
    ensure
      ENV.delete("CONFIGURATION_DIRECTORY")
      FileUtils.rm_rf(config_dir)
    end
  end

  it "LAVINMQ_CONFIGURATION_DIRECTORY takes precedence over CONFIGURATION_DIRECTORY" do
    config_dir = File.tempname("lavinmq-conf")
    Dir.mkdir_p(config_dir)
    File.write(File.join(config_dir, "lavinmq.ini"), "[main]\nlog_level = fatal\n")
    systemd_dir = File.tempname("lavinmq-systemd")
    Dir.mkdir_p(systemd_dir)
    File.write(File.join(systemd_dir, "lavinmq.ini"), "[main]\nlog_level = fatal\n")
    begin
      ENV["CONFIGURATION_DIRECTORY"] = systemd_dir
      ENV["LAVINMQ_CONFIGURATION_DIRECTORY"] = config_dir
      config = LavinMQ::Config.new
      config.parse([] of String)
      # config_file should NOT point to the systemd_dir
      config.config_file.should_not contain systemd_dir
    ensure
      ENV.delete("CONFIGURATION_DIRECTORY")
      ENV.delete("LAVINMQ_CONFIGURATION_DIRECTORY")
      FileUtils.rm_rf(config_dir)
      FileUtils.rm_rf(systemd_dir)
    end
  end

  it "accepts deprecated [http] section as alias for [mgmt]" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        data_dir = /tmp/lavinmq-spec
        [http]
        port = 15699
      CONFIG
    end
    io = IO::Memory.new
    config = LavinMQ::Config.new(io)
    argv = ["-c", config_file.path]
    config.parse(argv)
    config.http_port.should eq 15699
    io.to_s.should match(/\[http\] is deprecated/)
  end

  it "does not parse config before printing version information" do
    config_dir = File.tempname("lavinmq-conf")
    Dir.mkdir_p(config_dir)
    File.write(File.join(config_dir, "lavinmq.ini"), <<-CONFIG
      [http]
      port = 15699
      CONFIG
    )
    io = IO::Memory.new
    config = LavinMQ::Config.new(io)
    begin
      ENV["LAVINMQ_CONFIGURATION_DIRECTORY"] = config_dir
      ex = expect_raises(SpecExit) { config.parse(["--version"]) }
      ex.code.should eq 0
      io.to_s.should be_empty
    ensure
      ENV.delete("LAVINMQ_CONFIGURATION_DIRECTORY")
      FileUtils.rm_rf(config_dir)
    end
  end

  it "will not parse ini sections that do not exist" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [invalid_section]
        some_option = value
      CONFIG
    end
    config = LavinMQ::Config.new
    argv = ["-c", config_file.path]
    expect_raises(Exception) { config.parse(argv) }
  end

  it "will not raise error when parseing ini option that does not exist in section" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        invalid_option = value
      CONFIG
    end
    config = LavinMQ::Config.new(IO::Memory.new)
    argv = ["-c", config_file.path]
    config.parse(argv)
  end

  it "will not parse cli sections that do not exist" do
    # CLI parser raises an error for invalid options
    config = LavinMQ::Config.new
    argv = ["--nonexistent-flag=value"]
    expect_raises(OptionParser::InvalidOption) { config.parse(argv) }
  end

  it "parses pidfile from config" do
    config_file = File.tempfile do |file|
      file.print <<-CONFIG
        [main]
        log_level = fatal
        data_dir = /tmp/lavinmq-spec
        pidfile = /tmp/lavinmq.pid
      CONFIG
    end
    config = LavinMQ::Config.new
    argv = ["-c", config_file.path]
    config.parse(argv)
    config.pidfile.should eq "/tmp/lavinmq.pid"
  end

  it "should set amqp, http and mqtt bind with -b flag" do
    config = LavinMQ::Config.new
    argv = ["-b", "0.0.0.0"]
    config.parse(argv)
    config.amqp_bind.should eq "0.0.0.0"
    config.http_bind.should eq "0.0.0.0"
    config.mqtt_bind.should eq "0.0.0.0"
  end

  describe "stats_interval" do
    it "accepts sub-second values" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [main]
          stats_interval = 500
        CONFIG
      end
      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.stats_interval.should eq 500
    end

    it "rejects non-positive values" do
      [0, -5000].each do |ms|
        config_file = File.tempfile do |file|
          file.print <<-CONFIG
            [main]
            stats_interval = #{ms}
          CONFIG
        end
        config = LavinMQ::Config.new
        expect_raises(LavinMQ::Config::Error, /stats_interval/) do
          config.parse(["-c", config_file.path])
        end
      end
    end
  end

  describe "reload" do
    it "keeps the running config when the new config has an invalid value" do
      config_file = File.tempfile("lavinmq-config", ".ini")
      begin
        File.write(config_file.path, "[main]\nstats_interval = 5000\n")
        config = LavinMQ::Config.new
        config.parse(["-c", config_file.path])
        config.stats_interval.should eq 5000

        File.write(config_file.path, "[main]\nstats_interval = 0\n")
        expect_raises(LavinMQ::Config::Error, /stats_interval/) { config.reload }
        config.stats_interval.should eq 5000 # unchanged
      ensure
        File.delete?(config_file.path)
        Log.setup(:fatal)
      end
    end

    it "keeps the running config when the new config has an unknown section" do
      config_file = File.tempfile("lavinmq-config", ".ini")
      begin
        File.write(config_file.path, "[main]\nlog_level = warn\n")
        config = LavinMQ::Config.new
        config.parse(["-c", config_file.path])
        config.log_level.should eq ::Log::Severity::Warn

        File.write(config_file.path, "[bogus]\nfoo = bar\n")
        expect_raises(LavinMQ::Config::Error, /Unknown configuration section/) { config.reload }
        config.log_level.should eq ::Log::Severity::Warn # unchanged
      ensure
        File.delete?(config_file.path)
        Log.setup(:fatal)
      end
    end

    it "keeps the running config when the new config has an unwritable log_file" do
      config_file = File.tempfile("lavinmq-config", ".ini")
      begin
        File.write(config_file.path, "[main]\nlog_level = warn\n")
        config = LavinMQ::Config.new
        config.parse(["-c", config_file.path])
        config.log_level.should eq ::Log::Severity::Warn

        # The parent directory does not exist, so opening the log file fails.
        File.write(config_file.path, "[main]\nlog_level = error\nlog_file = /nonexistent/lavinmq.log\n")
        expect_raises(LavinMQ::Config::Error, /log_file/) { config.reload }
        config.log_level.should eq ::Log::Severity::Warn # unchanged, not half-applied
      ensure
        File.delete?(config_file.path)
        Log.setup(:fatal)
      end
    end

    it "does not half-apply a config when a later value is invalid" do
      config_file = File.tempfile("lavinmq-config", ".ini")
      begin
        File.write(config_file.path, "[main]\nstats_log_size = 120\n")
        config = LavinMQ::Config.new
        config.parse(["-c", config_file.path])
        config.stats_log_size.should eq 120

        # stats_log_size is valid, segment_size is not an integer and raises
        File.write(config_file.path, "[main]\nstats_log_size = 999\nsegment_size = notanumber\n")
        expect_raises(LavinMQ::Config::Error) { config.reload }
        config.stats_log_size.should eq 120 # the valid value was not applied either
      ensure
        File.delete?(config_file.path)
        Log.setup(:fatal)
      end
    end

    it "applies SNI changes on a successful reload" do
      config_file = File.tempfile("lavinmq-config", ".ini")
      begin
        File.write(config_file.path, <<-INI)
        [sni:foobar.localhost]
        tls_cert = spec/resources/foobar_localhost_certificate.pem
        tls_key = spec/resources/foobar_localhost_key.pem
        INI
        config = LavinMQ::Config.new
        config.parse(["-c", config_file.path])
        config.sni_manager.get_host("foobar.localhost").should_not be_nil
        config.sni_manager.get_host("test.example.com").should be_nil

        File.write(config_file.path, <<-INI)
        [sni:*.example.com]
        tls_cert = spec/resources/wildcard_example_certificate.pem
        tls_key = spec/resources/wildcard_example_key.pem
        INI
        config.reload

        # reload swaps in a fresh SNIManager: the new host resolves, the old one is gone.
        config.sni_manager.get_host("test.example.com").should_not be_nil
        config.sni_manager.get_host("foobar.localhost").should be_nil
      ensure
        File.delete?(config_file.path)
        Log.setup(:fatal)
      end
    end

    it "keeps serving traffic after a failed reload" do
      with_amqp_server do |s|
        config = LavinMQ::Config.instance
        original_config_file = config.config_file
        original_stats_interval = config.stats_interval
        config_file = File.tempfile("lavinmq-config", ".ini")
        begin
          File.write(config_file.path, "[main]\nstats_interval = 0\n")
          config.config_file = config_file.path
          expect_raises(LavinMQ::Config::Error) { config.reload }
          config.stats_interval.should eq original_stats_interval

          # The broker is still healthy: a publish/consume roundtrip works
          with_channel(s) do |ch|
            q = ch.queue
            q.publish_confirm("msg")
            q.get(no_ack: true).try(&.body_io.to_s).should eq "msg"
          end
        ensure
          config.config_file = original_config_file
          File.delete?(config_file.path)
          Log.setup(:fatal)
        end
      end
    end
  end

  describe "tcp_proxy_protocol" do
    {% for value, expected in {"1": true, "yes": true, "2": true, "-1": false, "no": false, "false": false, "0": false} %}
      it "sets tcp_proxy_protocol to {{expected}} when value is {{value}}" do
        config_file = File.tempfile do |file|
          file.print <<-CONFIG
                [amqp]
                tcp_proxy_protocol = {{value}}
              CONFIG
        end
        config = LavinMQ::Config.new
        argv = ["-c", config_file.path]
        config.parse(argv)
        config.tcp_proxy_protocol?.should eq {{expected}}
      end
    {% end %}
  end
end

# Connect a TLS client requesting *servername* to a one-shot server using
# *server_ctx*, and return the CN of the certificate the server presented.
private def served_cn(server_ctx : OpenSSL::SSL::Context::Server, servername : String) : String?
  tcp_server = TCPServer.new("127.0.0.1", 0)
  port = tcp_server.local_address.port
  spawn do
    if client = tcp_server.accept?
      begin
        OpenSSL::SSL::Socket::Server.new(client, server_ctx, sync_close: true).close
      rescue
        # ignore handshake errors, the client assertion will surface them
      ensure
        client.close rescue nil
      end
    end
  end
  Fiber.yield
  tcp_client = TCPSocket.new("127.0.0.1", port)
  client_ctx = OpenSSL::SSL::Context::Client.new
  client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
  ssl_client = OpenSSL::SSL::Socket::Client.new(tcp_client, client_ctx, hostname: servername)
  cn = ssl_client.peer_certificate.try(&.subject.to_a.to_h["CN"]?)
  ssl_client.close
  tcp_client.close
  cn
ensure
  tcp_server.try &.close
end

private def with_launcher(ini : String, &)
  data_dir = File.tempname("lavinmq", "reload-spec")
  Dir.mkdir_p data_dir
  config_file = File.tempfile("lavinmq", ".ini")
  begin
    File.write(config_file.path, ini)
    config = LavinMQ::Config.new
    config.parse(["-c", config_file.path])
    config.data_dir = data_dir
    config.data_dir_lock = false
    yield LavinMQ::Launcher.new(config), config, config_file
  ensure
    config_file.delete
    FileUtils.rm_rf data_dir
  end
end

# Reload behaviour that lives in the launcher: TLS/SNI changes are applied for
# the supported cases and warn that a restart is required for the rest.
describe LavinMQ::Launcher do
  describe "config reload" do
    it "serves the configured SNI certificate, and a rotated one after reload" do
      with_launcher(<<-INI) do |launcher, _config, config_file|
      [main]
      tls_cert = spec/resources/server_certificate.pem
      tls_key = spec/resources/server_key.pem

      [sni:foobar.localhost]
      tls_cert = spec/resources/foobar_localhost_certificate.pem
      tls_key = spec/resources/foobar_localhost_key.pem
      INI
        amqp_ctx = launcher.@amqp_tls_context.not_nil!
        served_cn(amqp_ctx, "foobar.localhost").should eq "foobar.localhost"
        served_cn(amqp_ctx, "other.example.com").should eq "anders" # default cert

        # Rotate the SNI host's certificate and reload.
        File.write(config_file.path, <<-INI)
        [main]
        tls_cert = spec/resources/server_certificate.pem
        tls_key = spec/resources/server_key.pem

        [sni:foobar.localhost]
        tls_cert = spec/resources/server_certificate.pem
        tls_key = spec/resources/server_key.pem
        INI
        launcher.reload!
        served_cn(amqp_ctx, "foobar.localhost").should eq "anders"
      end
    end

    it "serves a per-host certificate for an SNI host added on reload" do
      with_launcher(<<-INI) do |launcher, _config, config_file|
      [main]
      tls_cert = spec/resources/server_certificate.pem
      tls_key = spec/resources/server_key.pem
      INI
        amqp_ctx = launcher.@amqp_tls_context.not_nil!
        served_cn(amqp_ctx, "foobar.localhost").should eq "anders" # default cert, no SNI host yet

        # Add an SNI host and reload, as a SIGHUP would.
        File.write(config_file.path, <<-INI)
        [main]
        tls_cert = spec/resources/server_certificate.pem
        tls_key = spec/resources/server_key.pem

        [sni:foobar.localhost]
        tls_cert = spec/resources/foobar_localhost_certificate.pem
        tls_key = spec/resources/foobar_localhost_key.pem
        INI
        launcher.reload!
        served_cn(amqp_ctx, "foobar.localhost").should eq "foobar.localhost"
      end
    end

    it "warns that enabling TLS requires a restart" do
      with_launcher("[main]\nstats_interval = 5000\n") do |launcher, _config, config_file|
        launcher.@amqp_tls_context.should be_nil
        File.write(config_file.path, <<-INI)
        [main]
        tls_cert = spec/resources/server_certificate.pem
        tls_key = spec/resources/server_key.pem
        INI
        Log.capture("lmq.launcher", :warn) do |logs|
          launcher.reload!
          logs.check(:warn, /Enabling TLS requires a restart/)
        end
        launcher.@amqp_tls_context.should be_nil
      end
    end

    it "warns that disabling TLS requires a restart" do
      with_launcher(<<-INI) do |launcher, _config, config_file|
      [main]
      tls_cert = spec/resources/server_certificate.pem
      tls_key = spec/resources/server_key.pem
      INI
        launcher.@amqp_tls_context.should_not be_nil
        File.write(config_file.path, "[main]\ntls_cert =\n")
        Log.capture("lmq.launcher", :warn) do |logs|
          launcher.reload!
          logs.check(:warn, /Disabling TLS requires a restart/)
        end
      end
    end
  end
end
