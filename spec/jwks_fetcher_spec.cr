require "./spec_helper"

# Stub IdP serving OIDC discovery and a JWKS built from the given key entries.
# Each entry is a Hash representing a single JWK as served on the /jwks endpoint.
private def with_jwks_idp_server(keys : Array, & : Socket::IPAddress ->)
  bound = [] of Socket::IPAddress
  server = ::HTTP::Server.new do |ctx|
    base = "http://#{bound.first}"
    if ctx.request.path == "/.well-known/openid-configuration"
      ctx.response.content_type = "application/json"
      {issuer: base, jwks_uri: "#{base}/jwks"}.to_json(ctx.response)
    elsif ctx.request.path == "/jwks"
      ctx.response.content_type = "application/json"
      {keys: keys}.to_json(ctx.response)
    else
      ctx.response.status_code = 404
    end
  end
  bound << server.bind_tcp("127.0.0.1", 0)
  spawn { server.listen }
  Fiber.yield
  yield bound.first
ensure
  server.try &.close
end

# A valid base64url-encoded RSA modulus so to_pem succeeds.
private def rsa_modulus : String
  Base64.urlsafe_encode(Random::Secure.random_bytes(256), padding: false)
end

private def fetcher_for(addr) : LavinMQ::Auth::JWT::JWKSFetcher
  LavinMQ::Auth::JWT::JWKSFetcher.new(URI.parse("http://#{addr}"), 1.hour)
end

describe LavinMQ::Auth::JWT::JWKSFetcher do
  describe "#fetch_jwks alg filtering" do
    it "includes RSA signing keys with no alg param" do
      keys = [{kty: "RSA", use: "sig", kid: "no-alg", n: rsa_modulus, e: "AQAB"}]
      with_jwks_idp_server(keys) do |addr|
        result = fetcher_for(addr).fetch_jwks
        result.keys.keys.should eq ["no-alg"]
      end
    end

    it "includes RSA signing keys with alg RS256" do
      keys = [{kty: "RSA", use: "sig", alg: "RS256", kid: "rs256", n: rsa_modulus, e: "AQAB"}]
      with_jwks_idp_server(keys) do |addr|
        result = fetcher_for(addr).fetch_jwks
        result.keys.keys.should eq ["rs256"]
      end
    end

    it "excludes RSA signing keys with a non-RS256 alg" do
      keys = [{kty: "RSA", use: "sig", alg: "RS512", kid: "rs512", n: rsa_modulus, e: "AQAB"}]
      with_jwks_idp_server(keys) do |addr|
        result = fetcher_for(addr).fetch_jwks
        result.keys.should be_empty
      end
    end
  end
end
