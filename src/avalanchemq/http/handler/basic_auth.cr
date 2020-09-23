require "http/server/handler"
require "../../user_store"
require "base64"

module AvalancheMQ
  module HTTP
    class BasicAuthHandler
      include ::HTTP::Handler

      def initialize(@user_store : UserStore, @log : Logger)
      end

      def internal_unix_socket?(context)
        context.request.remote_address.to_s == HTTP::INTERNAL_UNIX_SOCKET
      end

      def forbidden(context)
        context.response.status_code = 401
        unless context.request.path.starts_with?("/api/whoami")
          context.response.headers["WWW-Authenticate"] = %(Basic realm="Login Required")
        end
      end

      def valid_auth?(user, password)
        !user.tags.empty? && user.password.to_s.size > 0 &&
          user.password && user.password.not_nil!.verify(password)
      end

      def call(context)
        if !context.request.path.starts_with?("/api/")
          return call_next(context)
        end
        auth = context.request.headers.fetch("Authorization", nil)
        if auth && auth.starts_with? "Basic "
          base64 = auth[6..-1]
          begin
            username, password = Base64.decode_string(base64).split(":")
            if user = @user_store[username]?
              if valid_auth?(user, password)
                context.authenticated_username = username
                return call_next(context)
              end
            end
          rescue Base64::Error
          end
        elsif internal_unix_socket?(context)
          context.authenticated_username = UserStore::DIRECT_USER
          return call_next(context)
        end
        forbidden(context)
      end
    end
  end
end
