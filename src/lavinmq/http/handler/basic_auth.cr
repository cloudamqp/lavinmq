require "http/server/handler"
require "base64"

module LavinMQ
  module HTTP
    class BasicAuthHandler
      include ::HTTP::Handler

      def initialize(@server : LavinMQ::Server, @log : Log)
      end

      def call(context)
        if public_path?(context.request.path)
          return call_next(context)
        end

        if internal_unix_socket?(context)
          context.authenticated_username = UserStore::DIRECT_USER
          return call_next(context)
        end

        if auth = context.request.headers.fetch("Authorization", nil)
          if auth.starts_with? "Basic "
            base64 = auth[6..-1]
            begin
              username, password = Base64.decode_string(base64).split(":")
              if user = @server.users[username]?
                if valid_auth?(user, password) && guest_only_loopback?(context, user)
                  context.authenticated_username = username
                  return call_next(context)
                end
              end
            rescue Base64::Error
            end
          end
        end
        forbidden(context)
      end

      private def public_path?(request_path) : Bool
        !request_path.starts_with?("/api/") &&
          request_path != "/metrics" &&
          !request_path.starts_with?("/metrics/")
      end

      private def internal_unix_socket?(context) : Bool
        context.request.remote_address.to_s == HTTP::INTERNAL_UNIX_SOCKET
      end

      private def forbidden(context)
        context.response.status_code = 401
        unless context.request.path.starts_with?("/api/whoami")
          context.response.headers["WWW-Authenticate"] = %(Basic realm="Login Required")
        end
      end

      private def valid_auth?(user, password) : Bool
        return false if user.tags.empty?
        return false if user.password.to_s.empty?
        user.password.not_nil!.verify(password)
      end

      private def guest_only_loopback?(context, user) : Bool
        return true unless user.name == "guest"
        return true unless Config.instance.guest_only_loopback?
        case context.request.remote_address
        when Socket::IPAddress
          return context.request.remote_address.as(Socket::IPAddress).loopback?
        when Socket::UNIXAddress
          return true
        end
        false
      end
    end
  end
end
