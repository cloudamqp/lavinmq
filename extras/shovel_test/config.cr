require "option_parser"
require "uri"

module ShovelTest
  class Config
    property host = "localhost"
    property amqp_port = 5672
    property http_port = 15672
    property user = "guest"
    property password = "guest"
    property vhost = "shovel-test"
    # Set when the vhost came from the command line; it is then left in place.
    property? vhost_given = false
    property messages = 10_000
    # How the broker reaches this process' HTTP endpoint. Change it when the
    # broker runs on another host.
    property endpoint_host = "localhost"
    property endpoint_port = 0
    property only : String?
    property? keep = false
    property timeout = 60.seconds

    def self.parse(argv = ARGV) : Config
      cfg = new
      ENV["AMQP_URL"]?.try { |url| cfg.apply_amqp_url(url) }
      OptionParser.parse(argv) do |p|
        p.banner = "Usage: shovel-test [options]\n\nRuns the shovel scenarios against a running LavinMQ."
        p.on("--amqp-url=URL", "amqp://user:pass@host:port/vhost (also read from $AMQP_URL)") { |v| cfg.apply_amqp_url(v) }
        p.on("--host=HOST", "Server host (default localhost)") { |v| cfg.host = v }
        p.on("--amqp-port=PORT", "AMQP port (default 5672)") { |v| cfg.amqp_port = v.to_i }
        p.on("--http-port=PORT", "Management HTTP port (default 15672)") { |v| cfg.http_port = v.to_i }
        p.on("--user=USER", "Username (default guest)") { |v| cfg.user = v }
        p.on("--password=PASS", "Password (default guest)") { |v| cfg.password = v }
        p.on("--vhost=VHOST", "Vhost to use; created if missing (default shovel-test, created and deleted)") do |v|
          cfg.vhost = v
          cfg.vhost_given = true
        end
        p.on("--messages=N", "Messages published in the bulk scenarios (default 10000)") { |v| cfg.messages = v.to_i }
        p.on("--endpoint-host=HOST", "Host the broker uses to reach the HTTP endpoint served here (default localhost)") { |v| cfg.endpoint_host = v }
        p.on("--endpoint-port=PORT", "Port for the HTTP endpoint (default: any free port)") { |v| cfg.endpoint_port = v.to_i }
        p.on("--only=NAME", "Run only scenarios whose name contains NAME") { |v| cfg.only = v }
        p.on("--timeout=SEC", "Seconds to wait for each expectation (default 60)") { |v| cfg.timeout = v.to_f.seconds }
        p.on("--keep", "Keep the vhost and everything in it afterwards") { cfg.keep = true }
        p.on("-h", "--help", "Show help") do
          puts p
          exit 0
        end
      end
      cfg
    end

    def apply_amqp_url(url : String) : Nil
      uri = URI.parse(url)
      @host = uri.host || @host
      @amqp_port = uri.port || @amqp_port
      @user = uri.user || @user
      @password = uri.password || @password
      path = uri.path.lchop("/")
      return if path.empty?
      @vhost = URI.decode(path)
      @vhost_given = true
    end

    # The URI the shovel (inside the broker) uses for its AMQP Source and
    # Destination — the same server, seen from the outside as we see it.
    def amqp_url : String
      "amqp://#{@user}:#{@password}@#{@host}:#{@amqp_port}/#{URI.encode_path_segment(@vhost)}"
    end

    def http_base : String
      "http://#{@host}:#{@http_port}"
    end
  end
end
