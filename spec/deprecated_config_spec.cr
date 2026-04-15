require "./spec_helper"
require "../src/lavinmq/config"

# Test helpers for deprecated option specs - added via reopen so macros
# have access to Config's instance vars and annotations.
module LavinMQ
  class Config
    # Returns the INI section and key for each deprecated INI option
    def self.deprecated_ini_info : Hash(String, {String, String})
      {% begin %}
      {
        {% for ivar in @type.instance_vars %}
          {% anno = ivar.annotation(IniOpt) %}
          {% if anno && anno[:deprecated] %}
            {{ivar.name.stringify}} => { {{anno[:section]}}, {{(anno[:ini_name] || ivar.name).stringify}} },
          {% end %}
        {% end %}
      }
      {% end %}
    end

    # Returns the CLI long flag for each deprecated CLI option
    def self.deprecated_cli_info : Hash(String, String)
      {% begin %}
      {
        {% for ivar in @type.instance_vars %}
          {% anno = ivar.annotation(CliOpt) %}
          {% if anno && anno[:deprecated] %}
            {{ivar.name.stringify}} => {{anno.args[1]}},
          {% end %}
        {% end %}
      }
      {% end %}
    end

    # Read any config value by ivar name (for dynamic forwarding checks)
    def get_value(name : String) : String
      {% begin %}
      case name
      {% for ivar in @type.instance_vars %}
      when {{ivar.name.stringify}} then @{{ivar.name}}.to_s
      {% end %}
      else raise "Unknown config option: #{name}"
      end
      {% end %}
    end

    # Returns names of all instance variables marked as deprecated (via CliOpt or IniOpt)
    def self.deprecated_option_names : Set(String)
      {% begin %}
        Set{
          {% for ivar in @type.instance_vars %}
            {% if (ivar.annotation(IniOpt) && ivar.annotation(IniOpt)[:deprecated]) ||
                    (ivar.annotation(CliOpt) && ivar.annotation(CliOpt)[:deprecated]) %}
              {{ivar.name.stringify}},
            {% end %}
          {% end %}
        }
      {% end %}
    end
  end
end

# Deprecated option forwarding map.
# Each entry maps a deprecated ivar name to its forwarding target and a test value.
# If a new deprecated annotation is added without an entry here, the
# completeness check will fail.
DEPRECATED_FORWARDS = {
  "default_password"               => {target: "default_password_hash", value: "8Yw8kj5HkhfRxQ/3kbTAO/nmgqGpkvMsGDbUWXA6+jTF3JP3"},
  "guest_only_loopback"            => {target: "default_user_only_loopback", value: "false"},
  "amqp_tls_cert"                  => {target: "tls_cert_path", value: "/tmp/test-cert.pem"},
  "amqp_tls_key"                   => {target: "tls_key_path", value: "/tmp/test-key.pem"},
  "mgmt_tls_cert"                  => {target: "tls_cert_path", value: "/tmp/test-cert.pem"},
  "mgmt_tls_key"                   => {target: "tls_key_path", value: "/tmp/test-key.pem"},
  "amqp_set_timestamp"             => {target: "set_timestamp", value: "true"},
  "amqp_consumer_timeout"          => {target: "consumer_timeout", value: "3600"},
  "amqp_default_consumer_prefetch" => {target: "default_consumer_prefetch", value: "500"},
}

describe LavinMQ::Config, "deprecated options" do
  it "has entries in DEPRECATED_FORWARDS for all deprecated options" do
    LavinMQ::Config.deprecated_option_names.should eq DEPRECATED_FORWARDS.keys.to_set
  end

  it "forwards all deprecated INI options to their replacements" do
    ini_info = LavinMQ::Config.deprecated_ini_info
    ini_info.each do |deprecated, (section, key)|
      next unless forward = DEPRECATED_FORWARDS[deprecated]?
      config_file = File.tempfile do |file|
        file.print "[#{section}]\n#{key} = #{forward[:value]}"
      end
      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      actual = config.get_value(forward[:target])
      unless actual == forward[:value]
        fail "Deprecated INI option #{key} in [#{section}] should forward to #{forward[:target]}: " \
             "expected #{forward[:value].inspect}, got #{actual.inspect}"
      end
    ensure
      config_file.try &.delete
    end
  end

  it "forwards all deprecated CLI options to their replacements" do
    cli_info = LavinMQ::Config.deprecated_cli_info
    cli_info.each do |deprecated, flag|
      next unless forward = DEPRECATED_FORWARDS[deprecated]?
      clean_flag = flag.split("=").first
      config = LavinMQ::Config.new
      config.parse([clean_flag, forward[:value]])
      actual = config.get_value(forward[:target])
      unless actual == forward[:value]
        fail "Deprecated CLI flag #{clean_flag} should forward to #{forward[:target]}: " \
             "expected #{forward[:value].inspect}, got #{actual.inspect}"
      end
    end
  end

  it "logs warning for deprecated INI options" do
    ini_info = LavinMQ::Config.deprecated_ini_info
    ini_info.each do |deprecated, (section, key)|
      next unless entry = DEPRECATED_FORWARDS[deprecated]?
      value = entry[:value]
      config_file = File.tempfile do |file|
        file.print "[#{section}]\n#{key} = #{value}"
      end
      io = IO::Memory.new
      config = LavinMQ::Config.new(io)
      config.parse(["-c", config_file.path])
      io.to_s.should match(/deprecated/i)
    ensure
      config_file.try &.delete
    end
  end

  it "logs warning for deprecated CLI options" do
    cli_info = LavinMQ::Config.deprecated_cli_info
    cli_info.each do |deprecated, flag|
      next unless entry = DEPRECATED_FORWARDS[deprecated]?
      value = entry[:value]
      clean_flag = flag.split("=").first
      io = IO::Memory.new
      config = LavinMQ::Config.new(io)
      config.parse([clean_flag, value])
      io.to_s.should match(/deprecated/i)
    end
  end
end
