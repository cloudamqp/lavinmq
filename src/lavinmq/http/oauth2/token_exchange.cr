require "http/client"
require "json"

module LavinMQ
  module HTTP
    module OAuth2
      # Parses an integer that may arrive as a JSON number or a JSON string,
      # returning nil for unparseable values instead of raising. Microsoft
      # Entra ID / Azure AD return `expires_in` as a string, which would
      # otherwise abort the whole token exchange.
      module FlexibleIntConverter
        def self.from_json(pull : JSON::PullParser) : Int64?
          case pull.kind
          when .int?    then pull.read_int
          when .string? then pull.read_string.to_i64?
          else               pull.skip; nil
          end
        end

        def self.to_json(value : Int64?, json : JSON::Builder)
          value.to_json(json)
        end
      end

      class TokenExchange
        struct TokenResponse
          include JSON::Serializable

          property access_token : String
          property token_type : String?
          @[JSON::Field(converter: FlexibleIntConverter)]
          property expires_in : Int64?
          property id_token : String?
        end

        def initialize(@token_endpoint : String, @client_id : String)
        end

        def exchange(code : String, redirect_uri : String, code_verifier : String) : TokenResponse
          uri = URI.parse(@token_endpoint)
          form = ::URI::Params.build do |p|
            p.add "grant_type", "authorization_code"
            p.add "code", code
            p.add "redirect_uri", redirect_uri
            p.add "client_id", @client_id
            p.add "code_verifier", code_verifier
          end

          ::HTTP::Client.new(uri) do |client|
            client.connect_timeout = 5.seconds
            client.read_timeout = 10.seconds
            response = client.post(
              uri.request_target,
              headers: ::HTTP::Headers{"Content-Type" => "application/x-www-form-urlencoded"},
              body: form
            )
            unless response.success?
              raise "Token exchange failed: HTTP #{response.status_code}"
            end
            TokenResponse.from_json(response.body)
          end
        end
      end
    end
  end
end
