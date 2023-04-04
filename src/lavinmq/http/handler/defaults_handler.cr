require "http/server/handler"

module LavinMQ
  module HTTP
    class ApiDefaultsHandler
      include ::HTTP::Handler

      def call(context)
        if context.request.path.starts_with?("/api/")
          context.response.content_type = "application/json"
          context.response.headers.add("Cache-Control", "private,max-age=5")
        end
        call_next(context)
      end
    end
  end
end
