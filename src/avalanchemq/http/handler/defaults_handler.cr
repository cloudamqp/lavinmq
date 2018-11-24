require "http/server/handler"

module AvalancheMQ
  module HTTP
    class ApiDefaultsHandler
      include ::HTTP::Handler

      def call(context)
        if context.request.path.starts_with?("/api/")
          context.response.content_type = "application/json"
        end
        call_next(context)
      end
    end
  end
end
