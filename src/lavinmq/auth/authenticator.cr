module LavinMQ
  module Auth
    abstract class Authenticator
      Log = LavinMQ::Log.for "auth.handler"
      @log = Logger.new(Log)
      property successor : Authenticator?

      abstract def authenticate(username : String, password : String)

      def set_successor(service : Authenticator) : Authenticator
        @successor = service
        service
      end

      def try_next(username : String, password : String)
        if successor = @successor
          successor.authenticate(username, password)
        else
          nil
        end
      end
    end
  end
end
