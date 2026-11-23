require "http/server"
require "./config"

module ShovelTest
  # The HTTP Destination for the HTTP scenarios, served by this process.
  #
  # The last path segment names the status to answer with, so scenarios can
  # keep their own hit counts apart with a prefix:
  #   /<anything>/200            -> 200
  #   /<anything>/503            -> 503
  #   /<anything>/flaky/404/10   -> 404 ten times, then 200
  # Anything else answers 200.
  class Endpoint
    getter port = 0

    def initialize(@cfg : Config)
      @hits = Hash(String, Int32).new(0)
      @lock = Mutex.new
      @server = HTTP::Server.new { |ctx| handle(ctx) }
    end

    def start : Nil
      @port = @server.bind_tcp("0.0.0.0", @cfg.endpoint_port).port
      spawn(name: "shovel-test endpoint") { @server.listen }
    end

    def stop : Nil
      @server.close unless @server.closed?
    end

    def url(path : String) : String
      "http://#{@cfg.endpoint_host}:#{@port}#{path}"
    end

    # Requests seen on `path` so far.
    def hits(path : String) : Int32
      @lock.synchronize { @hits[path] }
    end

    private def handle(ctx : HTTP::Server::Context) : Nil
      path = ctx.request.path
      ctx.request.body.try &.skip_to_end
      seen = @lock.synchronize { @hits[path] += 1 }
      status = status_for(path, seen)
      ctx.response.status_code = status
      ctx.response.content_type = "text/plain"
      ctx.response.print(status == 200 ? "ok" : "status #{status}")
    end

    private def status_for(path : String, seen : Int32) : Int32
      case path
      when %r{/flaky/(\d{3})/(\d+)\z}
        seen <= $2.to_i ? $1.to_i : 200
      when %r{/(\d{3})\z}
        $1.to_i
      else
        200
      end
    end
  end
end
