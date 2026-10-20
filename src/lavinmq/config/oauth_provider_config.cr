require "uri"

module LavinMQ
  # Settings for verifying JWTs from one OAuth 2.0 / OpenID Connect provider:
  # where to fetch its signing keys and how to map its token claims to
  # LavinMQ users and permissions, and optionally how the management UI logs
  # in with it (SSO). Configured in an [oauth.<name>] section, or in [oauth]
  # for the provider named "default".
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
    # Management UI SSO, used by the first provider that sets client_id or mgmt_base_url
    property client_id : String?
    property mgmt_base_url : URI?
    property mgmt_scopes : String

    def initialize(@issuer_url : URI,
                   @name : String = DEFAULT_NAME,
                   @resource_server_id : String? = nil,
                   @preferred_username_claims : Array(String) = ["sub", "client_id"],
                   @additional_scopes_keys : Array(String) = Array(String).new,
                   @scope_prefix : String? = nil,
                   @verify_aud : Bool = true,
                   @audience : String? = nil,
                   @jwks_cache_ttl : Time::Span = 1.hours,
                   @client_id : String? = nil,
                   @mgmt_base_url : URI? = nil,
                   @mgmt_scopes : String = "openid profile")
    end
  end
end
