module LavinMQ
  module Auth
    abstract class Authenticator
      Log = LavinMQ::Log.for "auth.handler"

      abstract def authenticate(username : String, password : String) : User?

      def authenticate(username : String, password : Bytes) : User?
        authenticate(username, String.new(password))
      end
    end
  end
end
