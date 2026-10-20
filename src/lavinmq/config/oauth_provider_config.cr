require "uri"

module LavinMQ
  # Settings for verifying JWTs from one OAuth 2.0 / OpenID Connect provider:
  # where to fetch its signing keys and how to map its token claims to
  # LavinMQ users and permissions. Configured in an [oauth.<name>] section, or
  # in [oauth] for the provider named "default".
  class OAuthProviderConfig
    DEFAULT_NAME = "default"

    getter name : String
    property issuer_url : URI
    property resource_server_id : String?
    property preferred_username_claims : Array(String)
    property additional_scopes_keys : Array(String)
    property scope_prefix : String?
    property? verify_aud : Bool
    property audience : String?
    property jwks_cache_ttl : Time::Span

    def initialize(@issuer_url : URI,
                   @name : String = DEFAULT_NAME,
                   @resource_server_id : String? = nil,
                   @preferred_username_claims : Array(String) = ["sub", "client_id"],
                   @additional_scopes_keys : Array(String) = Array(String).new,
                   @scope_prefix : String? = nil,
                   @verify_aud : Bool = true,
                   @audience : String? = nil,
                   @jwks_cache_ttl : Time::Span = 1.hours)
    end
  end
end
