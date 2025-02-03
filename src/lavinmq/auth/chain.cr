require "./authenticator"
require "./authenticators/basic"

module LavinMQ
  module Auth
    class Chain < Authenticator
      @backends : Array(Authenticator)

      def initialize(backends : Array(Authenticator))
        @backends = backends
      end

      def self.create(users : UserStore) : Chain
        backends = Config.instance.auth_backends
        authenticators = Array(Authenticator).new
        if backends.nil? || backends.empty?
          authenticators << BasicAuthenticator.new(users)
        else
          backends.each do |backend|
            case backend
            when "basic"
              authenticators << BasicAuthenticator.new(users)
            else
              raise "Unsupported authentication backend: #{backend}"
            end
          end
        end
        self.new(authenticators)
      end

      def authenticate(username : String, password : String)
        @backends.each do |backend|
          if user = backend.authenticate(username, password)
            return user
          end
        end
        nil
      end
    end
  end
end
