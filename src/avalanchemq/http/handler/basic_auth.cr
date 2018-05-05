require "http/server/handler"
require "base64"

module AvalancheMQ
  class BasicAuthHandler
    include HTTP::Handler

    def initialize(@user_store : UserStore, @log : Logger)
    end

    def call(context)
      auth = context.request.headers.fetch("Authorization", nil)
      if auth && auth.starts_with? "Basic "
        base64 = auth[6..-1]
        begin
          username, password = Base64.decode_string(base64).split(":")
          @log.debug { "auth? #{username}" }
          if user = @user_store[username]?
            if user.password.to_s.size > 0 && user.password == password
              context.authenticated_username = username
              return call_next(context)
            end
          end
        rescue Base64::Error
        end
      end
      context.response.status_code = 401
      context.response.headers["WWW-Authenticate"] = %(Basic realm="Login Required")
    end
  end
end
