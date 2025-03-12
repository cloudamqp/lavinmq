require "log"
require "./lavinmq/version"
require "./lavinmqperf/amqp/*"
require "./lavinmqperf/mqtt/*"
require "./stdlib/resource"

{% unless flag?(:release) %}
  STDERR.puts "WARNING: #{PROGRAM_NAME} not built in release mode"
{% end %}

Log.setup_from_env

module LavinMQPerf
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
    else                         abort Perf.new.banner
    end
  when "mqtt"
    mode = ARGV.shift?
    case mode
    when "throughput"       then pp "no mqtt throughput yet"
    else                         abort Perf.new.banner
    end
  else abort Perf.new.banner
  end
end
