require "../authenticator"
require "../../server"

module LavinMQ
  module Auth
    class RabbitBackendAuthenticator < Authenticator
      def initialize(@users : UserStore)
      end

      def authenticate(username : String, password : Bytes) : User?
        user = @users[username]
        return user if user && rabbit_backend_authenticate?(username, String.new(slice: password))
      rescue ex : Exception
        Log.error { "Rabbit Http authentication failed: #{ex.message}" }
      end

      def rabbit_backend_authenticate?(username : String, password : String) : Bool
        client = RabbitBackendClient.new()
        client.authenticate?(username, password)
      end
    end

    class RabbitBackendClient
      def initialize(@endpoint : String = Config.instance.rabbit_backend_url)
      end

      def authenticate?(username : String, password : String) : Bool
        ::HTTP::Client.post("#{@endpoint}#{Config.instance.rabbit_backend_user_path}",
          headers: ::HTTP::Headers{"Content-Type" => "application/json"},
          body: payload(username, password)).success?
      end

      def payload(username : String, password : String)
        {
          "username" => username,
          "password" => password,
        }.to_json
      end
    end
  end
end
