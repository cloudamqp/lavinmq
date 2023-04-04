require "http/server/handler"

module LavinMQ
  module HTTP
    class StrictTransportSecurity
      include ::HTTP::Handler

      def call(context)
        context.response.headers.add("Strict-Transport-Security", "max-age=31536000")
        call_next(context)
      end
    end
  end
end
