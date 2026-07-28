# Test HTTP endpoint for shovel disposition testing.
#
#   crystal run shovel_test_endpoint.cr -- --port 8888
#
# Point a shovel's dest-uri at a path that names the status you want:
#
#   http://localhost:8888/200   -> 2xx            => Confirmed (acked)
#   http://localhost:8888/503   -> 5xx/408/429    => Retry     (requeue + backoff)
#   http://localhost:8888/400   -> 400/422        => Reject    (reject(requeue:false) -> DLX)
#   http://localhost:8888/404   -> 404/401/403/.. => Abort     (error-out after threshold)
#   http://localhost:8888/418   -> any unknown    => Abort
#   http://localhost:8888/      -> 200
#
# It also supports a "flaky" path that fails N times then succeeds, handy for
# watching backoff recover:
#
#   http://localhost:8888/flaky/503/3  -> 503 three times, then 200
#
# Every request is logged with method, path, the status returned, and body.

require "http/server"
require "option_parser"

port = 8888
OptionParser.parse do |p|
  p.on("--port PORT", "Port to listen on (default 8888)") { |v| port = v.to_i }
end

flaky_seen = Hash(String, Int32).new(0)

server = HTTP::Server.new do |ctx|
  req = ctx.request
  path = req.path
  body = req.body.try(&.gets_to_end) || ""

  status =
    case path
    when %r{\A/flaky/(\d+)/(\d+)\z}
      code, limit = $1.to_i, $2.to_i
      n = (flaky_seen[path] += 1)
      n <= limit ? code : 200
    when %r{\A/(\d{3})\z}
      $1.to_i
    else
      200
    end

  ctx.response.status_code = status
  ctx.response.content_type = "text/plain"
  ctx.response.print(status == 200 ? "ok" : "status #{status}")

  shovel = req.headers["X-Shovel"]?
  STDOUT.puts "#{req.method} #{path} -> #{status}" \
              "#{shovel ? " [shovel=#{shovel}]" : ""}" \
              " body=#{body.inspect}"
end

addr = server.bind_tcp("0.0.0.0", port)
puts "shovel test endpoint listening on http://#{addr}"
puts "  /200 Confirmed  /503 Retry  /400 Reject  /404 Abort  /flaky/503/3"
server.listen
