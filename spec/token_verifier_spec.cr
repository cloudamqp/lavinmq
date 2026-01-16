require "./spec_helper"
require "../src/lavinmq/auth/token_verifier"
require "jwt"

# Helper class to expose private methods for testing
class TestableTokenVerifier < LavinMQ::Auth::TokenVerifier
  def public_extract_username(payload)
    extract_username(payload)
  end

  def public_parse_roles(payload)
    parse_roles(payload)
  end

  def public_extract_scopes_from_claim(claim)
    extract_scopes_from_claim(claim)
  end

  def public_filter_scopes(scopes)
    filter_scopes(scopes)
  end

  def public_parse_role(role, tags, permissions)
    parse_role(role, tags, permissions)
  end

  def public_parse_tag_role(role, tags)
    parse_tag_role(role, tags)
  end

  def public_parse_permission_role(role, permissions)
    parse_permission_role(role, permissions)
  end

  def public_determine_expected_audience
    determine_expected_audience
  end

  def public_validate_audience_strict(payload)
    validate_audience_strict(payload)
  end
end

def create_test_verifier(config : LavinMQ::Config)
  fetcher = JWT::JWKSFetcher.new("https://auth.example.com", 1.hour)
  TestableTokenVerifier.new(config, fetcher)
end

describe LavinMQ::Auth::TokenVerifier do
  describe "#extract_username" do
    it "extracts username from preferred_username claim" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username", "email"]
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"preferred_username": "john.doe"}))

      username = verifier.public_extract_username(payload)
      username.should eq "john.doe"
    end

    it "extracts username from email claim if preferred_username not present" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username", "email"]
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"email": "john@example.com"}))

      username = verifier.public_extract_username(payload)
      username.should eq "john@example.com"
    end

    it "raises error when no username claim found" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username", "email"]
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"sub": "12345"}))

      expect_raises(JWT::DecodeError, /No username found/) do
        verifier.public_extract_username(payload)
      end
    end

    it "uses first matching claim from list" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_preferred_username_claims = ["preferred_username", "email", "sub"]
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"email": "john@example.com", "sub": "12345"}))

      username = verifier.public_extract_username(payload)
      username.should eq "john@example.com"
    end
  end

  describe "#parse_tag_role" do
    it "parses valid tag roles" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      tags = Set(LavinMQ::Tag).new

      verifier.public_parse_tag_role("tag:administrator", tags)

      tags.should contain(LavinMQ::Tag::Administrator)
    end

    it "parses monitoring tag" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      tags = Set(LavinMQ::Tag).new

      verifier.public_parse_tag_role("tag:monitoring", tags)

      tags.should contain(LavinMQ::Tag::Monitoring)
    end

    it "ignores invalid tag names" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      tags = Set(LavinMQ::Tag).new

      verifier.public_parse_tag_role("tag:invalid_tag", tags)

      tags.should be_empty
    end
  end

  describe "#parse_permission_role" do
    it "parses configure permission" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      permissions = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

      verifier.public_parse_permission_role("configure:*:*", permissions)

      permissions.has_key?("/").should be_true
      permissions["/"][:config].should eq(/.*/)
    end

    it "parses read permission" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      permissions = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

      verifier.public_parse_permission_role("read:*:^queue", permissions)

      permissions.has_key?("/").should be_true
      permissions["/"][:read].should eq(/^queue/)
    end

    it "parses write permission" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      permissions = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

      verifier.public_parse_permission_role("write:*:^exchange", permissions)

      permissions.has_key?("/").should be_true
      permissions["/"][:write].should eq(/^exchange/)
    end

    it "converts wildcard vhost to /" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      permissions = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

      verifier.public_parse_permission_role("configure:*:*", permissions)

      permissions.has_key?("/").should be_true
    end

    it "converts wildcard pattern to .*" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      permissions = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

      verifier.public_parse_permission_role("configure:*:*", permissions)

      permissions.has_key?("/").should be_true
      permissions["/"][:config].should eq(/.*/)
    end

    it "handles custom vhost" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      permissions = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

      verifier.public_parse_permission_role("configure:test:*", permissions)

      permissions.has_key?("test").should be_true
    end

    it "ignores invalid permission type" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      permissions = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

      verifier.public_parse_permission_role("invalid:/:.*", permissions)

      permissions.should be_empty
    end

    it "ignores malformed role with wrong number of parts" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      permissions = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

      verifier.public_parse_permission_role("configure", permissions)

      permissions.should be_empty
    end

    it "handles invalid regex pattern gracefully" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      permissions = Hash(String, LavinMQ::Auth::BaseUser::Permissions).new

      verifier.public_parse_permission_role("configure:*:[invalid", permissions)

      # Should not add permission due to invalid regex
      # But may have initialized the vhost entry
      if permissions.has_key?("/")
        # If entry exists, the config regex should still be the default (not updated)
        permissions["/"][:config].should eq(/^$/)
      end
    end
  end

  describe "#filter_scopes" do
    it "filters scopes with explicit prefix" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_scope_prefix = "mq."
      verifier = create_test_verifier(config)

      scopes = ["mq.configure:*:.*", "mq.read:*:.*", "other.scope"]

      filtered = verifier.public_filter_scopes(scopes)

      filtered.should eq ["configure:*:.*", "read:*:.*"]
    end

    it "uses resource_server_id as prefix when scope_prefix is empty" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_resource_server_id = "lavinmq"
      config.oauth_scope_prefix = ""
      verifier = create_test_verifier(config)

      scopes = ["lavinmq.configure:*:.*", "lavinmq.read:*:.*", "other.scope"]

      filtered = verifier.public_filter_scopes(scopes)

      filtered.should eq ["configure:*:.*", "read:*:.*"]
    end

    it "returns all scopes when no prefix configured" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_scope_prefix = ""
      config.oauth_resource_server_id = ""
      verifier = create_test_verifier(config)

      scopes = ["configure:*:.*", "read:*:.*", "write:*:.*"]

      filtered = verifier.public_filter_scopes(scopes)

      filtered.should eq scopes
    end
  end

  describe "#extract_scopes_from_claim" do
    it "extracts scopes from string claim" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      claim = JSON.parse(%("configure:*:.*"))

      scopes = verifier.public_extract_scopes_from_claim(claim)

      scopes.should eq ["configure:*:.*"]
    end

    it "extracts multiple scopes from space-separated string" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      claim = JSON.parse(%("configure:*:.*  read:*:.*"))

      scopes = verifier.public_extract_scopes_from_claim(claim)

      scopes.should eq ["configure:*:.*", "read:*:.*"]
    end

    it "extracts scopes from array claim" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      claim = JSON.parse(%(["configure:*:.*", "read:*:.*"]))

      scopes = verifier.public_extract_scopes_from_claim(claim)

      scopes.should eq ["configure:*:.*", "read:*:.*"]
    end

    it "extracts scopes from hash claim when resource_server_id matches" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_resource_server_id = "lavinmq"
      verifier = create_test_verifier(config)

      claim = JSON.parse(%({"lavinmq": "configure:*:.*"}))

      scopes = verifier.public_extract_scopes_from_claim(claim)

      scopes.should eq ["configure:*:.*"]
    end

    it "extracts scopes from all keys in hash when resource_server_id is empty" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_resource_server_id = ""
      verifier = create_test_verifier(config)

      claim = JSON.parse(%({"service1": "scope1", "service2": "scope2"}))

      scopes = verifier.public_extract_scopes_from_claim(claim)

      scopes.should contain("scope1")
      scopes.should contain("scope2")
    end

    it "returns empty array for unsupported claim types" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      verifier = create_test_verifier(config)

      claim = JSON.parse(%(123))

      scopes = verifier.public_extract_scopes_from_claim(claim)

      scopes.should be_empty
    end
  end

  describe "#parse_roles" do
    it "parses tags from scope string" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_scope_prefix = ""
      config.oauth_resource_server_id = ""
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"scope": "tag:administrator tag:monitoring"}))

      tags, permissions = verifier.public_parse_roles(payload)

      tags.should contain(LavinMQ::Tag::Administrator)
      tags.should contain(LavinMQ::Tag::Monitoring)
    end

    it "parses permissions from scope string" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_scope_prefix = ""
      config.oauth_resource_server_id = ""
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"scope": "configure:*:*"}))

      tags, permissions = verifier.public_parse_roles(payload)

      permissions.has_key?("/").should be_true
      permissions["/"][:config].should eq(/.*/)
    end

    it "parses roles from resource_access claim" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_resource_server_id = ""
      config.oauth_scope_prefix = ""
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"resource_access": {"lavinmq": {"roles": ["configure:*:.*"]}}}))

      tags, permissions = verifier.public_parse_roles(payload)

      # Without resource_server_id, resource_access isn't parsed
      permissions.should be_empty
    end

    it "combines scopes from multiple sources" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_resource_server_id = "lavinmq"
      config.oauth_scope_prefix = ""
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"scope": "lavinmq.tag:administrator", "resource_access": {"lavinmq": {"roles": ["lavinmq.configure:*:*"]}}}))

      tags, permissions = verifier.public_parse_roles(payload)

      tags.should contain(LavinMQ::Tag::Administrator)
      permissions.has_key?("/").should be_true
      permissions["/"][:config].should eq(/.*/)
    end

    it "uses additional_scopes_key when configured" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_additional_scopes_key = "custom_permissions"
      config.oauth_scope_prefix = ""
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"custom_permissions": "configure:*:*"}))

      tags, permissions = verifier.public_parse_roles(payload)

      permissions.has_key?("/").should be_true
      permissions["/"][:config].should eq(/.*/)
    end
  end

  describe "#determine_expected_audience" do
    it "returns oauth_audience when set" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_audience = "my-api"
      config.oauth_resource_server_id = "lavinmq"
      verifier = create_test_verifier(config)

      audience = verifier.public_determine_expected_audience

      audience.should eq "my-api"
    end

    it "returns resource_server_id when oauth_audience is empty" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_audience = ""
      config.oauth_resource_server_id = "lavinmq"
      verifier = create_test_verifier(config)

      audience = verifier.public_determine_expected_audience

      audience.should eq "lavinmq"
    end

    it "returns empty string when both are empty" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_audience = ""
      config.oauth_resource_server_id = ""
      verifier = create_test_verifier(config)

      audience = verifier.public_determine_expected_audience

      audience.should eq ""
    end
  end

  describe "#validate_audience_strict" do
    it "raises error when token has audience but no expected audience configured" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_audience = ""
      config.oauth_resource_server_id = ""
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"aud": "some-audience"}))

      expect_raises(JWT::DecodeError, /Token contains audience claim but no expected audience is configured/) do
        verifier.public_validate_audience_strict(payload)
      end
    end

    it "does not raise when token has no audience claim" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_audience = ""
      config.oauth_resource_server_id = ""
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"sub": "12345"}))

      # Should not raise
      verifier.public_validate_audience_strict(payload)
    end

    it "does not raise when token has audience and expected audience is configured" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = "https://auth.example.com"
      config.oauth_audience = "my-api"
      verifier = create_test_verifier(config)

      payload = JSON.parse(%({"aud": "my-api"}))

      # Should not raise
      verifier.public_validate_audience_strict(payload)
    end
  end

  describe "TokenClaims" do
    it "creates record with all fields" do
      username = "testuser"
      tags = [LavinMQ::Tag::Administrator] of LavinMQ::Tag
      permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
      expires_at = Time.utc + 1.hour

      claims = LavinMQ::Auth::TokenClaims.new(username, tags, permissions, expires_at)

      claims.username.should eq username
      claims.tags.should eq tags
      claims.permissions.should eq permissions
      claims.expires_at.should eq expires_at
    end
  end
end
