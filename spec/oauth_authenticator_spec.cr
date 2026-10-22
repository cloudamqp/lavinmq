require "./spec_helper"

# Stub IdP serving OIDC discovery and a JWKS with one RSA key, counting the
# requests it receives. `healthy: false` makes every response a 500.
private def with_stub_idp_server(healthy = true, & : Socket::IPAddress, Proc(Int32) ->)
  requests = Atomic(Int32).new(0)
  bound = [] of Socket::IPAddress
  server = ::HTTP::Server.new do |ctx|
    requests.add(1)
    base = "http://#{bound.first}"
    if !healthy
      ctx.response.status_code = 500
    elsif ctx.request.path == "/.well-known/openid-configuration"
      ctx.response.content_type = "application/json"
      {issuer: base, jwks_uri: "#{base}/jwks"}.to_json(ctx.response)
    elsif ctx.request.path == "/jwks"
      n = Base64.urlsafe_encode(Random::Secure.random_bytes(256), padding: false)
      ctx.response.content_type = "application/json"
      {keys: [{kty: "RSA", use: "sig", alg: "RS256", kid: "k1", n: n, e: "AQAB"}]}.to_json(ctx.response)
    else
      ctx.response.status_code = 404
    end
  end
  bound << server.bind_tcp("127.0.0.1", 0)
  spawn { server.listen }
  Fiber.yield
  # Atomic is a struct, so yield a reader rather than a copy of it
  yield bound.first, -> { requests.get }
ensure
  server.try &.close
end

private def authenticator_for_idp(addr) : {LavinMQ::Auth::OAuthAuthenticator, LavinMQ::Auth::JWT::JWKSFetcher}
  config = create_test_config
  config.oauth_issuer_url = URI.parse("http://#{addr}")
  fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(URI.parse("http://#{addr}"), 1.hour)
  provider = config.oauth_providers.first
  verifier = LavinMQ::Auth::JWT::TokenVerifier.new(provider, fetcher)
  {LavinMQ::Auth::OAuthAuthenticator.new(verifier), fetcher}
end

# Passes prevalidation (RS256, three parts, future exp) but has a bogus signature.
private def well_formed_jwt(issuer : String) : String
  header = Base64.urlsafe_encode(%({"alg":"RS256","typ":"JWT"}), padding: false)
  payload = Base64.urlsafe_encode({sub: "x", exp: 9999999999, iss: issuer}.to_json, padding: false)
  "#{header}.#{payload}.c2lnbmF0dXJl"
end

def create_test_authenticator(config : LavinMQ::Config? = nil)
  config ||= create_test_config
  # public_keys = LavinMQ::Auth::PublicKeys.new
  provider = config.oauth_providers.first
  jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(provider.issuer_url, provider.jwks_cache_ttl)
  verifier = LavinMQ::Auth::JWT::TokenVerifier.new(provider, jwks_fetcher)
  LavinMQ::Auth::OAuthAuthenticator.new(verifier)
end

def create_test_config
  config = LavinMQ::Config.new
  config.oauth_issuer_url = URI.parse("https://auth.example.com")
  config.oauth_preferred_username_claims = ["preferred_username"]
  config
end

# Skips signature verification and names the verifier's issuer as the user,
# so specs can see which provider a token was routed to.
private class IssuerEchoVerifier < LavinMQ::Auth::JWT::TokenVerifier
  getter parsed = 0

  def initialize(issuer : String)
    provider = LavinMQ::OAuthProviderConfig.new(URI.parse(issuer), name: issuer)
    super(provider, LavinMQ::Auth::JWT::JWKSFetcher.new(provider.issuer_url, 1.hour))
  end

  def parse_token(token : String) : LavinMQ::Auth::JWT::TokenClaim
    @parsed += 1
    LavinMQ::Auth::JWT::TokenClaim.new(issuer, [] of LavinMQ::Tag,
      {} of String => LavinMQ::Auth::BaseUser::Permissions, RoughTime.utc + 1.hour)
  end
end

private def multi_provider_authenticator(*issuers : String) : {LavinMQ::Auth::OAuthAuthenticator, Array(IssuerEchoVerifier)}
  verifiers = issuers.map { |issuer| IssuerEchoVerifier.new(issuer) }.to_a
  hash = verifiers.to_h { |v| {v.issuer, v.as(LavinMQ::Auth::JWT::TokenVerifier)} }
  {LavinMQ::Auth::OAuthAuthenticator.new(hash), verifiers}
end

private def oauth_context(issuer : String?) : LavinMQ::Auth::Context
  claims = {"sub" => "x", "exp" => 9999999999}
  claims["iss"] = issuer if issuer
  header = Base64.urlsafe_encode(%({"alg":"RS256","typ":"JWT"}), padding: false)
  payload = Base64.urlsafe_encode(claims.to_json, padding: false)
  LavinMQ::Auth::Context.new("", "#{header}.#{payload}.c2lnbmF0dXJl".to_slice, loopback: true)
end

private def parse_oauth_test_config(ini : String) : LavinMQ::Config
  config_file = File.tempfile("lavinmq-config", ".ini", &.print(ini))
  config = LavinMQ::Config.new(IO::Memory.new)
  config.parse(["-c", config_file.path])
  config
ensure
  config_file.try &.delete
  Log.setup(:fatal)
end

describe LavinMQ::Auth::OAuthAuthenticator do
  # Note: These tests focus on the prevalidation checks and error handling paths in the
  # OAuth authenticator that can be tested without a real JWKS endpoint. Full JWT validation
  # requires network access to fetch public keys, which would need integration tests with
  # a mock OAuth server.

  describe "on-demand JWKS fetch" do
    it "fetches keys on first authentication when the refresh loop hasn't populated them" do
      with_stub_idp_server do |addr, _request_count|
        authenticator, fetcher = authenticator_for_idp(addr)
        ctx = LavinMQ::Auth::Context.new("", well_formed_jwt("http://#{addr}").to_slice, loopback: true)
        authenticator.authenticate(ctx)
        fetcher.public_keys.empty?.should be_false
      end
    end

    it "rate-limits fetch attempts when the provider is unreachable" do
      with_stub_idp_server(healthy: false) do |addr, request_count|
        authenticator, _ = authenticator_for_idp(addr)
        ctx = LavinMQ::Auth::Context.new("", well_formed_jwt("http://#{addr}").to_slice, loopback: true)
        2.times { authenticator.authenticate(ctx) }
        request_count.call.should eq 1
      end
    end
  end

  describe "JWKS refresh loop" do
    it "stops on cleanup while it is retrying an unreachable provider" do
      with_stub_idp_server(healthy: false) do |addr, request_count|
        authenticator, fetcher = authenticator_for_idp(addr)
        fetcher.start_refresh_loop(retry_delay: 10.milliseconds)
        wait_for { request_count.call >= 2 }
        authenticator.cleanup
        sleep 50.milliseconds
        count = request_count.call
        sleep 100.milliseconds
        request_count.call.should eq count
      end
    end
  end

  describe "#reload" do
    it "fetches keys from the new issuer after the issuer changes" do
      with_stub_idp_server do |addr_a, _request_count_a|
        with_stub_idp_server do |addr_b, request_count_b|
          authenticator, old_fetcher = authenticator_for_idp(addr_a)
          old_verifier = authenticator.token_verifier
          config = create_test_config
          config.oauth_issuer_url = URI.parse("http://#{addr_b}")
          authenticator.reload(config)
          ctx = LavinMQ::Auth::Context.new("", well_formed_jwt("http://#{addr_b}").to_slice, loopback: true)
          authenticator.authenticate(ctx)
          request_count_b.call.should be > 0
          authenticator.token_verifier.should_not be old_verifier
          authenticator.token_verifier.fetcher.public_keys.empty?.should be_false
          old_fetcher.@stopped.value.should be_true
          # Token refreshes from connections authenticated with the old issuer are rejected
          expect_raises(LavinMQ::Auth::JWT::VerificationError, /disabled/) do
            old_verifier.verify_token(well_formed_jwt("http://#{addr_a}"))
          end
        ensure
          authenticator.try &.cleanup
        end
      end
    end

    it "reloads the verifier, keeping its fetcher and keys, when the issuer is unchanged" do
      with_stub_idp_server do |addr, _request_count|
        authenticator, fetcher = authenticator_for_idp(addr)
        fetcher.ensure_keys
        verifier = authenticator.token_verifier
        config = create_test_config
        config.oauth_issuer_url = URI.parse("http://#{addr}/")
        config.oauth_scope_prefix = "mq."
        config.oauth_jwks_cache_ttl = 5.minutes
        authenticator.reload(config)
        authenticator.token_verifier.should be verifier
        verifier.fetcher.should be fetcher
        verifier.provider.scope_prefix.should eq "mq."
        fetcher.default_cache_ttl.should eq 5.minutes
        fetcher.public_keys.empty?.should be_false
      end
    end
  end

  describe "with several providers" do
    it "verifies a token with the provider matching its issuer" do
      authenticator, verifiers = multi_provider_authenticator("https://a.example.com", "https://b.example.com")
      user = authenticator.authenticate(oauth_context("https://b.example.com/"))
      user = user.should be_a(LavinMQ::Auth::OAuthUser)
      user.name.should eq "https://b.example.com"
      verifiers.map(&.parsed).should eq [0, 1]
      # Token refreshes go to the same provider
      user.@verifier.should be verifiers[1]
    end

    it "rejects a token from an issuer without a provider" do
      authenticator, verifiers = multi_provider_authenticator("https://a.example.com", "https://b.example.com")
      authenticator.authenticate(oauth_context("https://evil.example.com")).should be_nil
      verifiers.map(&.parsed).should eq [0, 0]
    end

    it "rejects a token without an issuer" do
      authenticator, verifiers = multi_provider_authenticator("https://a.example.com")
      authenticator.authenticate(oauth_context(nil)).should be_nil
      verifiers.first.parsed.should eq 0
    end

    it "doesn't fetch keys for a token from an unknown issuer" do
      with_stub_idp_server do |addr, request_count|
        authenticator, _ = authenticator_for_idp(addr)
        authenticator.authenticate(oauth_context("https://evil.example.com"))
        request_count.call.should eq 0
      end
    end
  end

  describe "#sso_verifier" do
    sso_authenticator = ->(providers : Array(LavinMQ::OAuthProviderConfig)) {
      LavinMQ::Auth::OAuthAuthenticator.new(providers.to_h do |p|
        {p.name, LavinMQ::Auth::JWT::TokenVerifier.new(p, LavinMQ::Auth::JWT::JWKSFetcher.new(p.issuer_url, 1.hour))}
      end)
    }

    it "is the verifier of the first provider with SSO configured" do
      authenticator = sso_authenticator.call([
        LavinMQ::OAuthProviderConfig.new(URI.parse("https://a.example.com"), name: "a"),
        LavinMQ::OAuthProviderConfig.new(URI.parse("https://b.example.com"), name: "b",
          client_id: "ui-b", mgmt_base_url: URI.parse("https://mq.example.com")),
        LavinMQ::OAuthProviderConfig.new(URI.parse("https://c.example.com"), name: "c",
          client_id: "ui-c", mgmt_base_url: URI.parse("https://mq.example.com")),
      ])
      authenticator.sso_verifier.try(&.provider.name).should eq "b"
    end

    it "is nil when the first provider with SSO configured has invalid settings" do
      authenticator = sso_authenticator.call([
        LavinMQ::OAuthProviderConfig.new(URI.parse("https://a.example.com"), name: "a",
          client_id: "ui-a", mgmt_base_url: URI.parse("http://mq.example.com")),
        LavinMQ::OAuthProviderConfig.new(URI.parse("https://b.example.com"), name: "b",
          client_id: "ui-b", mgmt_base_url: URI.parse("https://mq.example.com")),
      ])
      authenticator.sso_verifier.should be_nil
    end
  end

  describe "#reload with several providers" do
    it "reloads kept providers, starts new ones and disables removed ones" do
      # Unreachable issuers: the refresh loops started for them fail fast
      a = "http://127.0.0.1:1/a"
      b = "http://127.0.0.1:1/b"
      authenticator = LavinMQ::Auth::OAuthAuthenticator.create(
        [LavinMQ::OAuthProviderConfig.new(URI.parse(a), name: "a")])
      verifier_a = authenticator.token_verifier
      begin
        authenticator.reload(parse_oauth_test_config("[oauth.a]\nissuer = #{a}\n[oauth.b]\nissuer = #{b}\n"))
        authenticator.verifiers.size.should eq 2
        authenticator.verifiers[0].should be verifier_a
        verifier_b = authenticator.verifiers[1]
        verifier_b.issuer.should eq b

        authenticator.reload(parse_oauth_test_config("[oauth.b]\nissuer = #{b}\n"))
        authenticator.verifiers.should eq [verifier_b]
        expect_raises(LavinMQ::Auth::JWT::VerificationError, /disabled/) do
          verifier_a.verify_token(String.new(oauth_context(a).password))
        end
      ensure
        authenticator.cleanup
      end
    end

    it "keeps the current providers when the config has none" do
      authenticator, verifiers = multi_provider_authenticator("https://a.example.com", "https://b.example.com")
      providers = verifiers.map &.provider
      authenticator.reload(LavinMQ::Config.new)
      authenticator.verifiers.should eq verifiers
      authenticator.verifiers.map(&.provider).should eq providers
    end
  end

  describe "#authenticate with invalid tokens" do
    it "rejects tokens that don't start with 'ey'" do
      authenticator = create_test_authenticator

      ctx = LavinMQ::Auth::Context.new("testuser", "not-a-jwt-token".to_slice, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with invalid JWT format (wrong number of parts)" do
      authenticator = create_test_authenticator

      # Only 2 parts instead of 3
      ctx = LavinMQ::Auth::Context.new("testuser", "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0".to_slice, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with more than 3 parts" do
      authenticator = create_test_authenticator

      # 4 parts
      ctx = LavinMQ::Auth::Context.new("testuser", "eyA.eyB.eyC.eyD".to_slice, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with missing algorithm in header" do
      authenticator = create_test_authenticator

      # Create token with no "alg" field
      header = {"typ" => "JWT"}
      payload = {"sub" => "test"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with non-RS256 algorithm" do
      authenticator = create_test_authenticator

      # Create token with HS256 algorithm (should be rejected)
      header = {"alg" => "HS256", "typ" => "JWT"}
      payload = {"sub" => "test", "exp" => (RoughTime.utc.to_unix + 3600).to_i64}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects tokens with missing exp claim" do
      authenticator = create_test_authenticator

      # Create token without exp claim
      header = {"alg" => "RS256", "typ" => "JWT"}
      payload = {"sub" => "test", "preferred_username" => "testuser"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects expired tokens in prevalidation" do
      authenticator = create_test_authenticator

      # Create token that expired 1 hour ago
      header = {"alg" => "RS256", "typ" => "JWT"}
      payload = {"sub" => "test", "exp" => (RoughTime.utc.to_unix - 3600).to_i64, "preferred_username" => "testuser"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.fake_signature".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end

    it "rejects token with None algorithm (security check)" do
      authenticator = create_test_authenticator

      # Try "none" algorithm attack
      header = {"alg" => "none", "typ" => "JWT"}
      payload = {"sub" => "test", "exp" => (RoughTime.utc.to_unix + 3600).to_i64, "preferred_username" => "hacker"}
      header_b64 = Base64.strict_encode(header.to_json).tr("+/", "-_").rstrip("=")
      payload_b64 = Base64.strict_encode(payload.to_json).tr("+/", "-_").rstrip("=")

      token = "#{header_b64}.#{payload_b64}.".to_slice
      ctx = LavinMQ::Auth::Context.new("testuser", token, loopback: true)
      user = authenticator.authenticate ctx
      user.should be_nil
    end
  end

  describe "configuration requirements" do
    it "accepts valid configuration" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]

      provider = config.oauth_providers.first
      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(provider.issuer_url, provider.jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(provider, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with multiple preferred username claims" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["email", "preferred_username", "sub"]

      provider = config.oauth_providers.first
      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(provider.issuer_url, provider.jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(provider, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with optional audience verification disabled" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_verify_aud = false

      provider = config.oauth_providers.first
      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(provider.issuer_url, provider.jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(provider, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with custom scope prefix" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_scope_prefix = "mq."

      provider = config.oauth_providers.first
      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(provider.issuer_url, provider.jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(provider, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end

    it "works with resource server ID" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://auth.example.com")
      config.oauth_preferred_username_claims = ["preferred_username"]
      config.oauth_resource_server_id = "lavinmq-api"

      provider = config.oauth_providers.first
      jwks_fetcher = LavinMQ::Auth::JWT::JWKSFetcher.new(provider.issuer_url, provider.jwks_cache_ttl)
      verifier = LavinMQ::Auth::JWT::TokenVerifier.new(provider, jwks_fetcher)
      authenticator = LavinMQ::Auth::OAuthAuthenticator.new(verifier)
      authenticator.should be_a(LavinMQ::Auth::OAuthAuthenticator)
    end
  end
end
