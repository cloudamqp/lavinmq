require "../auth_handler"
require "jwt"
require "../../config"
require "http/client"


module LavinMQ
  class OAuth2Handler < LavinMQ::AuthHandler
    def initialize(@users : UserStore)
    end

    # Temporary for tests
    @token : String = LavinMQ::Config.instance.token
    @public_key : String = LavinMQ::Config.instance.public_key


    def authenticate(username : String, password : String)
      begin
        fetch_jwks_token
        payload, header = JWT.decode(@token, key: @public_key, algorithm: JWT::Algorithm::RS256, verify: true)

        pp payload
        pp header
        oauth_user
      rescue ex : JWT::DecodeError
        @log.warn { "OAuth2 authentication failed, could not decode token: #{ex}" }
        try_next(username, password)
      rescue ex : JWT::UnsupportedAlgorithmError
        @log.warn { "OAuth2 authentication failed, unsupported algortihm: #{ex}" }
        try_next(username, password)
      rescue ex
        @log.warn { "OAuth2 authentication failed: #{ex}" }
        try_next(username, password)
      end
    end

    private def fetch_jwks_token
    end

    def oauth_user
      # Discuss ow to do this?
      # TODO: Create a uset that will be deleted when it disconnects, but also cannot be authorised with basic auth.
      # introduce the needed configs for validation, and parse the payload to get the user details
      user = @users.create("oauth_user", "password")
    end
  end
end
