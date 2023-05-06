require "http/server/handler"
require "base64"

module LavinMQ
  module HTTP
    class AuthHandler
      include ::HTTP::Handler

      def initialize(@server : LavinMQ::Server, @log : Log)
      end

      def call(context)
        if internal_unix_socket?(context)
          context.authenticated_username = UserStore::DIRECT_USER
          return call_next(context)
        end

        if auth = cookie_auth(context) || basic_auth(context)
          username, password = auth
          if valid_auth?(username, password, context.request.remote_address)
            context.authenticated_username = username
            return call_next(context)
          end
        end

        unauthenticated(context)
      end

      private def basic_auth(context)
        if auth = context.request.headers["Authorization"]?
          if auth.starts_with? "Basic "
            base64 = auth[6..]
            return decode(base64)
          end
        end
      end

      private def cookie_auth(context)
        if m = context.request.cookies["m"]?
          if idx = m.value.rindex(':')
            auth = URI.decode(m.value[idx + 1..])
            return decode(auth)
          end
        end
      end

      private def decode(base64) : Tuple(String, String)?
        string = Base64.decode_string(base64)
        if idx = string.index(':')
          username = string[0...idx]
          password = string[idx + 1..]
          return {username, password}
        end
      rescue Base64::Error
      end

      private def valid_auth?(username, password, remote_address) : Bool
        return false if password.empty?
        if user = @server.users[username]?
          if user_password = user.password
            if user_password.verify(password)
              if guest_only_loopback?(remote_address, username)
                return false if user.tags.empty?
                return true
              end
            end
          end
        end
        false
      end

      private def internal_unix_socket?(context) : Bool
        if addr = context.request.remote_address.as?(Socket::UNIXAddress)
          return addr.to_s == HTTP::INTERNAL_UNIX_SOCKET
        end
        false
      end

      private def unauthenticated(context)
        context.response.status_code = 401
      end

      private def guest_only_loopback?(remote_address, username) : Bool
        return true unless Config.instance.guest_only_loopback?
        return true unless username == "guest"
        case remote_address
        when Socket::IPAddress   then remote_address.loopback?
        when Socket::UNIXAddress then true
        else                          false
        end
      end
    end
  end
end
