require "http/server/handler"
require "base64"

module AvalancheMQ
  module HTTP
    class BasicAuthHandler
      include ::HTTP::Handler

      def initialize(@user_store : UserStore, @log : Logger)
      end

      def validate(context)
        context.request.path.starts_with?("/api/") ||
        context.request.path = "/metrics"
      end

      def call(context)
        unless validate(context)
          return call_next(context)
        end
        auth = context.request.headers.fetch("Authorization", nil)
        if auth && auth.starts_with? "Basic "
          base64 = auth[6..-1]
          begin
            username, password = Base64.decode_string(base64).split(":")
            if user = @user_store[username]?
              if !user.tags.empty? && user.password.to_s.size > 0 && user.password && user.password.not_nil!.verify(password)
                context.authenticated_username = username
                return call_next(context)
              end
            end
          rescue Base64::Error
          end
        end
        context.response.status_code = 401
        unless context.request.path.starts_with?("/api/whoami")
          context.response.headers["WWW-Authenticate"] = %(Basic realm="Login Required")
        end
      end
    end
  end
end
