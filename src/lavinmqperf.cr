{% if flag?(:gc_none) %}
  require "immix"
{% end %}
require "log"
require "./lavinmq/version"
require "./lavinmqperf/*"
require "./stdlib/resource"

{% unless flag?(:release) %}
  STDERR.puts "WARNING: #{PROGRAM_NAME} not built in release mode"
{% end %}

Log.setup_from_env

module LavinMQPerf
  mode = ARGV.shift?
  case mode
  when "throughput"       then Throughput.new.run
  when "bind-churn"       then BindChurn.new.run
  when "queue-churn"      then QueueChurn.new.run
  when "connection-churn" then ConnectionChurn.new.run
  when "channel-churn"    then ChannelChurn.new.run
  when "consumer-churn"   then ConsumerChurn.new.run
  when "connection-count" then ConnectionCount.new.run
  when "queue-count"      then QueueCount.new.run
  when /^.+$/             then Perf.new.run([mode.not_nil!])
  else                         abort Perf.new.banner
  end
end
