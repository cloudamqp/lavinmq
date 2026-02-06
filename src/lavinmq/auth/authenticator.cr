require "./context"

module LavinMQ
  module Auth
    abstract class Authenticator
      Log = LavinMQ::Log.for "auth.authenticator"

      abstract def authenticate(context : Context) : BaseUser?
      abstract def cleanup
    end
  end
end
