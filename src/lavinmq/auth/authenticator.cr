module LavinMQ
  module Auth
    abstract class Authenticator
      Log = LavinMQ::Log.for "auth.handler"

      abstract def authenticate(username : String, password : Bytes) : User?

      def authenticate(username : String, password : String) : User?
        authenticate(username, password.to_slice)
      end
    end
  end
end
