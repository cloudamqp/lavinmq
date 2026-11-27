require "uri"

module LavinMQ
  # Settings for verifying JWTs from one OAuth 2.0 / OpenID Connect provider:
  # where to fetch its signing keys and how to map its token claims to
  # LavinMQ users and permissions.
  class OAuthProviderConfig
    getter issuer_url : URI
    getter resource_server_id : String?
    getter preferred_username_claims : Array(String)
    getter additional_scopes_keys : Array(String)
    getter scope_prefix : String?
    getter? verify_aud : Bool
    getter audience : String?
    getter jwks_cache_ttl : Time::Span

    def initialize(@issuer_url : URI,
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
