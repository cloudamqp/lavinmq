require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class RabbitHttpAuthenticator < Authenticator
      def initialize(@users : UserStore)
      end

      def authenticate(username : String, password : Bytes) : User?
        user = @users[username]
        return user if user && rabbit_http_authenticate?(username, password)
      rescue ex : Exception
        Log.error { "Rabbit Http authentication failed: #{ex.message}" }
      end

      private def rabbit_http_authenticate?(username : String, password : Bytes) : Bool
        payload = {
          "username" => username,
          "password" => password.to_s,
        }.to_json

        ::HTTP::Client.post(Config.instance.auth_http_user_path,
          headers: ::HTTP::Headers{"Content-Type" => "application/json"},
          body: payload).success?
      end
    end
  end
end
