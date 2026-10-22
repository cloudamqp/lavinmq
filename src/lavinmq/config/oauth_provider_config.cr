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
    # Management UI SSO, used by the first provider where it's #sso_configured?
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

    # Whether any management UI SSO setting is set
    def sso_configured? : Bool
      !(@client_id.nil? && @mgmt_base_url.nil?)
    end

    # Whether management UI SSO is configured and its settings are valid
    def sso_enabled? : Bool
      sso_configured? && sso_disabled_reason.nil?
    end

    # Why SSO is off although it's configured, nil if it's on or not configured
    def sso_disabled_reason : String?
      return unless sso_configured?
      return "missing client_id in OAuth provider '#{@name}'" unless @client_id
      base_url = @mgmt_base_url
      return "missing mgmt_base_url in OAuth provider '#{@name}'" unless base_url
      unless mgmt_base_url_allowed?(base_url)
        "mgmt_base_url in OAuth provider '#{@name}' must use https:// or http://{localhost,127.0.0.1,[::1]}"
      end
    end

    private def mgmt_base_url_allowed?(uri : URI) : Bool
      return true if uri.scheme == "https"
      return false unless uri.scheme == "http"
      host = uri.host.try(&.downcase)
      {"localhost", "127.0.0.1", "::1", "[::1]"}.includes?(host)
    end
  end
end
