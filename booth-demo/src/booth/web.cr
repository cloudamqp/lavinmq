require "http/server"
require "./app"

module Booth
  class Web
    ASSETS = {
      "/"          => {"text/html; charset=utf-8", {{ read_file("#{__DIR__}/../../public/index.html") }}},
      "/app.js"    => {"text/javascript; charset=utf-8", {{ read_file("#{__DIR__}/../../public/app.js") }}},
      "/style.css" => {"text/css; charset=utf-8", {{ read_file("#{__DIR__}/../../public/style.css") }}},
    }

    def initialize(@app : App)
      @server = HTTP::Server.new { |ctx| handle(ctx) }
    end

    def listen(host : String, port : Int32) : Nil
      address = @server.bind_tcp(host, port)
      Log.info { "Dashboard on http://#{address}/ (operator controls: http://#{address}/?operator)" }
      @server.listen
    end

    private def handle(ctx : HTTP::Server::Context) : Nil
      req = ctx.request
      path = req.path
      case {req.method, path}
      when {"GET", "/events"}
        events(ctx)
      when {"GET", "/api/state"}
        json(ctx, @app.snapshot)
      when {"POST", "/api/reset"}
        @app.reset
        json(ctx, %({"ok":true}))
      when {"POST", "/api/transfer-leadership"}
        transfer_leadership(ctx)
      else
        if req.method == "POST" && (m = path.match(%r{\A/api/scenarios/(\w+)/(start|stop|rate)\z}))
          scenario_action(ctx, m[1], m[2])
        elsif req.method == "GET" && (asset = ASSETS[path]?)
          ctx.response.content_type = asset[0]
          ctx.response.headers["Cache-Control"] = "no-cache"
          ctx.response.print asset[1]
        else
          ctx.response.respond_with_status(:not_found)
        end
      end
    end

    # Server-sent events: the latest snapshot, a few times per second
    private def events(ctx) : Nil
      res = ctx.response
      res.content_type = "text/event-stream"
      res.headers["Cache-Control"] = "no-cache"
      res.headers["X-Accel-Buffering"] = "no"
      loop do
        res << "data: " << @app.snapshot << "\n\n"
        res.flush
        sleep App::SNAPSHOT_INTERVAL
      end
    rescue IO::Error
      # the browser went away
    end

    private def scenario_action(ctx, name, action) : Nil
      scenario = @app.scenario(name)
      return ctx.response.respond_with_status(:not_found) unless scenario
      case action
      when "start" then scenario.start
      when "stop"  then scenario.stop
      when "rate"
        value = ctx.request.query_params["value"]?.try &.to_i?
        return ctx.response.respond_with_status(:bad_request, "value required") unless value
        if scenario.is_a?(Stream) && ctx.request.query_params["reader"]? == "replay"
          scenario.replay_rate = value
        else
          scenario.rate = value
        end
      end
      json(ctx, scenario.to_json)
    end

    private def transfer_leadership(ctx) : Nil
      response = @app.monitor.transfer_leadership
      if response && response.status_code == 202
        @app.events.add("transfer", "Operator asked #{@app.monitor.leader.try &.name} to hand over leadership")
        json(ctx, response.body)
      else
        ctx.response.status_code = 502
        json(ctx, {error: response.try(&.body) || "leader unreachable"}.to_json)
      end
    end

    private def json(ctx, body : String) : Nil
      ctx.response.content_type = "application/json"
      ctx.response.print body
    end
  end
end
