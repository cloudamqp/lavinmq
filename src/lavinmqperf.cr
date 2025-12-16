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

# Default to 'amqp' if the first argument is not a protocol or an option
protocols = {"amqp", "mqtt"}
protocol = protocols.includes?(ARGV[0]?) ? ARGV.shift : "amqp"
case protocol
when "amqp"
  mode = ARGV.shift?
  case mode
  when "throughput"       then LavinMQPerf::AMQP::Throughput.new.run
  when "bind-churn"       then LavinMQPerf::AMQP::BindChurn.new.run
  when "queue-churn"      then LavinMQPerf::AMQP::QueueChurn.new.run
  when "connection-churn" then LavinMQPerf::AMQP::ConnectionChurn.new.run
  when "channel-churn"    then LavinMQPerf::AMQP::ChannelChurn.new.run
  when "consumer-churn"   then LavinMQPerf::AMQP::ConsumerChurn.new.run
  when "connection-count" then LavinMQPerf::AMQP::ConnectionCount.new.run
  when "queue-count"      then LavinMQPerf::AMQP::QueueCount.new.run
  when /^.+$/             then LavinMQPerf::Perf.new.run([mode.not_nil!])
  else                         abort LavinMQPerf::Perf.new.amqp_banner
  end
when "mqtt"
  mode = ARGV.shift?
  case mode
  when "throughput" then LavinMQPerf::MQTT::Throughput.new.run
  when /^.+$/       then LavinMQPerf::Perf.new.run([mode.not_nil!])
  else                   abort LavinMQPerf::Perf.new.mqtt_banner
  end
when /^.+$/
  LavinMQPerf::Perf.new.run([protocol.not_nil!])
else abort LavinMQPerf::Perf.new.amqp_banner
end
