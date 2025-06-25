require "log"
require "./lavinmq/version"
require "./lavinmqperf/amqp/*"
require "./lavinmqperf/mqtt/*"
require "./stdlib/resource"

{% unless flag?(:release) %}
  STDERR.puts "WARNING: #{PROGRAM_NAME} not built in release mode"
{% end %}

Signal::SEGV.reset # Let the OS generate a coredump
Log.setup_from_env

module LavinMQPerf
  protocol = ARGV[0]?
  # Default to 'amqp' if the first argument is not a protocol or an option
  ARGV.unshift("amqp") if ARGV[0]?.try { |a| a != "amqp" && a != "mqtt" && !a.starts_with?("-") } || ARGV.empty?
  protocol = ARGV.shift? || "amqp"
  case protocol
  when "amqp"
    mode = ARGV.shift?
    case mode
    when "throughput"       then AMQP::Throughput.new.run
    when "bind-churn"       then AMQP::BindChurn.new.run
    when "queue-churn"      then AMQP::QueueChurn.new.run
    when "connection-churn" then AMQP::ConnectionChurn.new.run
    when "channel-churn"    then AMQP::ChannelChurn.new.run
    when "consumer-churn"   then AMQP::ConsumerChurn.new.run
    when "connection-count" then AMQP::ConnectionCount.new.run
    when "queue-count"      then AMQP::QueueCount.new.run
    when /^.+$/             then Perf.new.run([mode.not_nil!])
    else                         abort Perf.new.amqp_banner
    end
  when "mqtt"
    mode = ARGV.shift?
    case mode
    when "throughput"       then MQTT::Throughput.new.run
    when /^.+$/             then Perf.new.run([mode.not_nil!])
    else                         abort Perf.new.mqtt_banner
    end
  when /^.+$/
    Perf.new.run([protocol.not_nil!])
  else abort Perf.new.amqp_banner
  end
end
