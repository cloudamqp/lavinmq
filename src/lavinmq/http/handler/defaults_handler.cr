require "http/server/handler"

module LavinMQ
  module HTTP
    class ApiDefaultsHandler
      include ::HTTP::Handler

      def call(context)
        context.response.content_type = "application/json"
        call_next(context)
      end
    end
  end
end
