require "http/server/handler"
require "base64"

module AvalancheMQ
  class BasicAuthHandler
    include HTTP::Handler

    def initialize(@user_store)
    end

    def call(context)
      auth = context.request.headers.fetch("Authorization", nil)
      if auth && auth.starts_with? "Basic "
        base64 = auth[6..-1]
        begin
          username, password = Base64.decode_string(base64).split(":")
          if user = @user_store[username]? || nil
            if user.password == password
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
