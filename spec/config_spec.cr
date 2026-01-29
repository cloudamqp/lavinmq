require "log/spec"
require "./spec_helper"
require "../src/lavinmq/config"

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

          [amqp]
          bind = 0.0.0.0
          port = 5673
          tls_port = 5674
          unix_path = /tmp/lavinmq.sock
          unix_proxy_protocol = 2
          tcp_proxy_protocol = 1
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

      # AMQP section
      config.amqp_bind.should eq "0.0.0.0"
      config.amqp_port.should eq 5673
      config.amqps_port.should eq 5674
      config.unix_path.should eq "/tmp/lavinmq.sock"
      config.unix_proxy_protocol.should eq 2
      config.tcp_proxy_protocol.should eq 1
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
      config.clustering_max_unsynced_actions.should eq 16384
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
    config.clustering_advertised_uri.should eq "lavinmq://test:5679"
    config.clustering_bind.should eq "0.0.0.0"
    config.clustering_etcd_endpoints.should eq "etcd1:2379,etcd2:2379"
    config.clustering_etcd_prefix.should eq "cli-prefix"
    config.clustering_max_unsynced_actions.should eq 4096
    config.clustering_port.should eq 5680
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
      config.clustering_max_unsynced_actions.should eq 2048
      config.clustering_port.should eq 5681
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
    config = LavinMQ::Config.new
    argv = ["-c", config_file.path]
    config.parse(argv)
  end

  it "will not parse cli sections that do not exist" do
    # CLI parser raises an error for invalid options
    config = LavinMQ::Config.new
    argv = ["--nonexistent-flag=value"]
    expect_raises(OptionParser::InvalidOption) { config.parse(argv) }
  end

  describe "with deprecated options" do
    it "should log warning for ini options" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
        [main]
        default_password = +pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+
      CONFIG
      end
      logs = Log.capture(level: :info) do
        config = LavinMQ::Config.new
        argv = ["-c", config_file.path]
        config.parse(argv)
      end
      logs.check(:warn, /is deprecated/)
    end

    it "should log warning for cli options" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
        [main]
      CONFIG
      end
      logs = Log.capture(level: :info) do
        config = LavinMQ::Config.new
        argv = ["-c", config_file.path, "--default-password", "8Yw8kj5HkhfRxQ/3kbTAO/nmgqGpkvMsGDbUWXA6+jTF3JP3"]
        config.parse(argv)
      end
      logs.check(:warn, /is deprecated/)
    end

    it "should forward ini option values to the new property" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
        [main]
        default_password = 8Yw8kj5HkhfRxQ/3kbTAO/nmgqGpkvMsGDbUWXA6+jTF3JP3
      CONFIG
      end
      config = LavinMQ::Config.new
      argv = ["-c", config_file.path]
      config.parse(argv)
      config.default_password_hash.to_s.should eq "8Yw8kj5HkhfRxQ/3kbTAO/nmgqGpkvMsGDbUWXA6+jTF3JP3"
    end

    it "should forward cli option values to the new property" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
        [main]
      CONFIG
      end
      config = LavinMQ::Config.new
      argv = ["-c", config_file.path, "--default-password", "8Yw8kj5HkhfRxQ/3kbTAO/nmgqGpkvMsGDbUWXA6+jTF3JP3"]
      config.parse(argv)
      config.default_password_hash.to_s.should eq "8Yw8kj5HkhfRxQ/3kbTAO/nmgqGpkvMsGDbUWXA6+jTF3JP3"
    end
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
end
