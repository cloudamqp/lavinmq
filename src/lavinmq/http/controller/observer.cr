require "../controller"
require "../../fiber_profiler"

module LavinMQ
  module HTTP
    class ObserverController < Controller
      private def register_routes
        get "/api/observer" do |context, _params|
          refuse_unless_administrator(context, user(context))
          context.response.content_type = "text/plain"
          context.response.headers["Cache-Control"] = "no-store"
          FiberProfiler.profile(context.response)
          context
        end
      end
    end
  end
end
