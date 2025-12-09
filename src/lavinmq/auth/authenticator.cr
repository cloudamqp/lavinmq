module LavinMQ
  module Auth
    abstract class Authenticator
      Log = LavinMQ::Log.for "auth.handler"

      abstract def authenticate(username : String, password : String) : BaseUser?

      def authenticate(username : String, password : Bytes) : BaseUser?
        authenticate(username, String.new(password))
      end
    end
  end
end
