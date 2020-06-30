require "http/server/handler"
require "http/status"
require "base64"
require "../resource_helpers"

module AvalancheMQ
  module HTTP
    class RateLimitHandler
      include ::HTTP::Handler

      RESPONSE_BODY = {error: "API rate limit exceeded"}.to_json

      def initialize(@rate_limiter : RateLimiter, @log : Logger)
      end

      def call(context)
        unless context.request.path.starts_with?("/api/")
          return call_next(context)
        end

        if allowed?(context)
          return call_next(context)
        end

        context.response.status_code = 403
        context.response.print(RESPONSE_BODY)
      end

      private def allowed?(context : ::HTTP::Server::Context) : Bool
        key = request_ip(context)
        @rate_limiter.allowed?(key)
      end

      private def request_ip(context) : String
        remote_address = context.request.remote_address

        case remote_address
        when Socket::IPAddress
          remote_address.address
        else
          remote_address.to_s
        end
      end
    end
  end
end
