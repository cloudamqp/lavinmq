require "json"
require "./lavinmq_wasm/router"

loop do
  line = STDIN.gets
  break unless line
  line = line.strip
  next if line.empty?

  begin
    request = JSON.parse(line)
    action = request["action"]?.try(&.as_s)

    result = case action
             when "route"
               queues = LavinMQWasm.route(request)
               {"matched_queues" => queues}.to_json
             when "match_policy"
               policies = Array(LavinMQWasm::Policy).from_json(request["policies"].to_json)
               resource_name = request["resource_name"].as_s
               resource_type = request["resource_type"].as_s
               policy, args = LavinMQWasm.match_policies(policies, resource_name, resource_type)
               {
                 "matched_policy" => policy,
                 "effective_args" => args,
               }.to_json
             else
               {"error" => "unknown action: #{action}"}.to_json
             end

    STDOUT.puts result
    STDOUT.flush
  rescue ex
    STDOUT.puts({"error" => ex.message}.to_json)
    STDOUT.flush
  end
end
