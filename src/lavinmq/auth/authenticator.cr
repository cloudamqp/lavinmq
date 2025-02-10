module LavinMQ
  module Auth
    abstract class Authenticator
      Log = LavinMQ::Log.for "auth.handler"

      abstract def authenticate(username : String, password : String) : User?
    end
  end
end
