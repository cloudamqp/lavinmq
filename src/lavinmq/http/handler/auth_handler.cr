require "http/server/handler"
require "base64"

module LavinMQ
  module HTTP
    class AuthHandler
      include ::HTTP::Handler

      def initialize(@authenticator : Auth::Authenticator, @direct_user : Auth::User, @internal_unix_socket_path : String)
      end

      def call(context)
        if internal_unix_socket?(context)
          context.user ||= @direct_user
        end

        # Explicit credentials override a user assigned earlier (direct user
        # or OAuth cookie session) and must be valid for the request to stay
        # authenticated. The passwordless OAuth identity cookie does not
        # count as credentials.
        if auth = explicit_credentials(context)
          username, password, vhost = auth
          context.user = authenticate(username, password, context.request.remote_address, vhost)
        end

        call_next(context)
      end

      # Header naming the vhost a user is scoped to, for clients using Basic auth
      VHOST_HEADER = "X-Vhost"
      # Prefix of the "m" cookie when the login is for a vhost scoped user:
      # `|v:<url encoded vhost>:<base64 credentials>`
      COOKIE_VHOST_PREFIX = "|v:"

      # Returns username, password and, for vhost scoped users, the vhost
      private def explicit_credentials(context) : Tuple(String, String, String?)?
        if auth = cookie_auth(context)
          return auth unless auth[1].empty?
        end
        basic_auth(context)
      end

      private def basic_auth(context) : Tuple(String, String, String?)?
        if auth = context.request.headers["Authorization"]?
          if auth.starts_with? "Basic "
            base64 = auth[6..]
            if creds = decode(base64)
              username, password = creds
              vhost = context.request.headers[VHOST_HEADER]?.presence
              {username, password, vhost}
            end
          end
        end
      end

      private def cookie_auth(context) : Tuple(String, String, String?)?
        if m = context.request.cookies["m"]?
          # The "|oauth:" identity cookie set for SSO sessions (see
          # OAuthController) carries no password and is not credentials.
          return if m.value.starts_with?("|oauth:")
          if idx = m.value.rindex(':')
            vhost = nil
            if m.value.starts_with?(COOKIE_VHOST_PREFIX)
              vhost = URI.decode(m.value[COOKIE_VHOST_PREFIX.size...idx]).presence
            end
            auth = URI.decode(m.value[idx + 1..])
            if creds = decode(auth)
              username, password = creds
              {username, password, vhost}
            end
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

      # With `vhost` a user scoped to that vhost is looked up first
      private def authenticate(username, password, remote_address, vhost : String? = nil) : Auth::BaseUser?
        return if password.empty?
        auth_context = LavinMQ::Auth::Context.new(
          username, password.to_slice, remote_address, vhost)
        user = @authenticator.authenticate(auth_context)
        return if user.nil?
        return if user.tags.empty?
        user
      end

      private def internal_unix_socket?(context) : Bool
        if addr = context.request.remote_address.as?(Socket::UNIXAddress)
          return addr.to_s == @internal_unix_socket_path
        end
        false
      end
    end
  end
end
