require "../controller"

module LavinMQ
  module HTTP
    class DiagnosticsController < Controller
      private def register_routes
        get "/api/diagnostics/fibers" do |context, _params|
          refuse_unless_administrator(context, user(context))
          fibers = Array(NamedTuple(name: String)).new
          Fiber.list do |f|
            next if f.dead?
            fibers << {name: f.name || "(unnamed)"}
          end
          fibers.to_json(context.response)
          context
        end
      end
    end
  end
end
