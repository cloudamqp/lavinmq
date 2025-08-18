require "http/server/handler"
require "base64"

module LavinMQ
  module HTTP
    class AuthRejector
      include ::HTTP::Handler

      def initialize(@server : LavinMQ::Server)
      end

      def call(context)
        if context.authenticated_username?
          call_next(context)
        else
          unauthenticated(context)
        end
      end

      private def unauthenticated(context)
        context.response.status_code = 401
      end
    end
  end
end
