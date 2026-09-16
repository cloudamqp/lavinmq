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
          username, password = auth
          context.user = authenticate(username, password, context.request.remote_address)
        end

        call_next(context)
      end

      # Users scoped to a vhost log in as `vhost/name`, see `authenticate`
      VHOST_DELIMITER = '/'

      private def explicit_credentials(context) : Tuple(String, String)?
        if auth = cookie_auth(context)
          return auth unless auth[1].empty?
        end
        basic_auth(context)
      end

      private def basic_auth(context)
        if auth = context.request.headers["Authorization"]?
          if auth.starts_with? "Basic "
            base64 = auth[6..]
            decode(base64)
          end
        end
      end

      private def cookie_auth(context)
        if m = context.request.cookies["m"]?
          # The "|oauth:" identity cookie set for SSO sessions (see
          # OAuthController) carries no password and is not credentials.
          return if m.value.starts_with?("|oauth:")
          if idx = m.value.rindex(':')
            auth = URI.decode(m.value[idx + 1..])
            decode(auth)
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

      # The username is first tried as a global user. If that fails and it
      # contains a slash it is read as `vhost/name` (split at the last slash,
      # so vhost names with slashes work: `//alice` is alice on vhost "/")
      # and tried as a user scoped to that vhost.
      private def authenticate(username, password, remote_address) : Auth::BaseUser?
        return if password.empty?
        if user = authenticate(username, password, remote_address, nil)
          return user
        end
        if idx = username.rindex(VHOST_DELIMITER)
          vhost = username[0...idx]
          name = username[idx + 1..]
          authenticate(name, password, remote_address, vhost) unless name.empty?
        end
      end

      private def authenticate(username, password, remote_address, vhost : String?) : Auth::BaseUser?
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
