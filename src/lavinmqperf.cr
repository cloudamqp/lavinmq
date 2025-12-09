require "log"
require "./lavinmq/version"
require "./lavinmqperf/amqp/*"
require "./lavinmqperf/mqtt/*"
require "./stdlib/*"

{% unless flag?(:release) %}
  STDERR.puts "WARNING: #{PROGRAM_NAME} not built in release mode"
{% end %}

Signal::SEGV.reset # Let the OS generate a coredump
Log.setup_from_env

module LavinMQPerf
  # Default to 'amqp' if the first argument is not a protocol or an option
  protocols = {"amqp", "mqtt"}
  protocol = protocols.includes?(ARGV[0]?) ? ARGV.shift : "amqp"
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
    when "throughput" then MQTT::Throughput.new.run
    when /^.+$/       then Perf.new.run([mode.not_nil!])
    else                   abort Perf.new.mqtt_banner
    end
  when /^.+$/
    Perf.new.run([protocol.not_nil!])
  else abort Perf.new.amqp_banner
  end
end
