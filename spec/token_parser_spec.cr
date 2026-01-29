require "./spec_helper"
require "../src/lavinmq/auth/jwt/token_verifier"

module TokenParserTestHelper
  extend self

  def create_token_parser(
    preferred_username_claims = ["preferred_username"],
    resource_server_id : String? = nil,
    scope_prefix : String? = nil,
    additional_scopes_key : String? = nil,
  ) : LavinMQ::Auth::JWT::TokenParser
    config = LavinMQ::Config.new
    config.oauth_issuer_url = URI.parse("https://auth.example.com")
    config.oauth_preferred_username_claims = preferred_username_claims
    config.oauth_resource_server_id = resource_server_id
    config.oauth_scope_prefix = scope_prefix
    config.oauth_additional_scopes_key = additional_scopes_key
    LavinMQ::Auth::JWT::TokenParser.new(config)
  end

  def create_mock_token(payload : LavinMQ::Auth::JWT::Payload) : LavinMQ::Auth::JWT::Token
    header = LavinMQ::Auth::JWT::Header.new(alg: "RS256", typ: "JWT")
    LavinMQ::Auth::JWT::Token.new(header, payload, Bytes.new(0))
  end
end

describe LavinMQ::Auth::JWT::TokenParser do
  describe "#extract_username" do
    it "extracts username from preferred_username claim" do
      parser = TokenParserTestHelper.create_token_parser(["preferred_username"])
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600)
      payload["preferred_username"] = JSON::Any.new("testuser")
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.username.should eq("testuser")
    end

    it "extracts username from sub claim" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600, sub: "subject-user")
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.username.should eq("subject-user")
    end

    it "extracts username from iss claim" do
      parser = TokenParserTestHelper.create_token_parser(["iss"])
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600, iss: "https://issuer.example.com")
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.username.should eq("https://issuer.example.com")
    end

    it "extracts username from first matching claim in the list" do
      parser = TokenParserTestHelper.create_token_parser(["email", "preferred_username", "sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600, sub: "sub-user")
      payload["preferred_username"] = JSON::Any.new("pref-user")
      payload["email"] = JSON::Any.new("email@example.com")
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.username.should eq("email@example.com")
    end

    it "falls back to subsequent claims if first is missing" do
      parser = TokenParserTestHelper.create_token_parser(["email", "preferred_username", "sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600, sub: "sub-user")
      payload["preferred_username"] = JSON::Any.new("pref-user")
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.username.should eq("pref-user")
    end

    it "falls back to sub when other claims are missing" do
      parser = TokenParserTestHelper.create_token_parser(["email", "preferred_username", "sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600, sub: "sub-user")
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.username.should eq("sub-user")
    end

    it "raises when no username claim is found" do
      parser = TokenParserTestHelper.create_token_parser(["email", "preferred_username"])
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600, sub: "sub-user")
      token = TokenParserTestHelper.create_mock_token(payload)
      expect_raises(LavinMQ::Auth::JWT::DecodeError, /No username found in JWT claims/) do
        parser.parse(token)
      end
    end

    it "extracts username from custom claim in unmapped fields" do
      parser = TokenParserTestHelper.create_token_parser(["custom_user_id"])
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600)
      payload["custom_user_id"] = JSON::Any.new("custom-user")
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.username.should eq("custom-user")
    end

    it "ignores non-string values in unmapped fields" do
      parser = TokenParserTestHelper.create_token_parser(["numeric_id", "fallback"])
      payload = LavinMQ::Auth::JWT::Payload.new(exp: RoughTime.utc.to_unix + 3600)
      payload["numeric_id"] = JSON::Any.new(12345_i64)
      payload["fallback"] = JSON::Any.new("fallback-user")
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.username.should eq("fallback-user")
    end
  end

  describe "#parse" do
    it "raises when exp claim is missing" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(sub: "user")
      token = TokenParserTestHelper.create_mock_token(payload)
      expect_raises(LavinMQ::Auth::JWT::DecodeError, /No expiration time found/) do
        parser.parse(token)
      end
    end

    it "sets expires_at from exp claim" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      exp_time = RoughTime.utc.to_unix + 7200
      payload = LavinMQ::Auth::JWT::Payload.new(exp: exp_time, sub: "user")
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.expires_at.should eq(Time.unix(exp_time))
    end
  end

  describe "#parse_roles" do
    describe "scope claim parsing" do
      it "parses scopes from space-separated scope claim" do
        parser = TokenParserTestHelper.create_token_parser(["sub"])
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user",
          scope: "read:myvhost/* write:myvhost/*"
        )
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions["myvhost"][:read].should eq(/.*/)
        claims.permissions["myvhost"][:write].should eq(/.*/)
      end

      it "returns empty permissions when scope is empty" do
        parser = TokenParserTestHelper.create_token_parser(["sub"])
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user"
        )
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions.should be_empty
        claims.tags.should be_empty
      end
    end

    describe "resource_access parsing" do
      it "extracts roles from resource_access when resource_server_id is set" do
        parser = TokenParserTestHelper.create_token_parser(["sub"], resource_server_id: "lavinmq")
        roles = LavinMQ::Auth::JWT::ResourceRoles.new(roles: ["lavinmq.read:myvhost/*"])
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user",
          resource_access: {"lavinmq" => roles}
        )
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions["myvhost"][:read].should eq(/.*/)
      end

      it "ignores resource_access for other servers" do
        parser = TokenParserTestHelper.create_token_parser(["sub"], resource_server_id: "lavinmq")
        other_roles = LavinMQ::Auth::JWT::ResourceRoles.new(roles: ["read:other/*"])
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user",
          resource_access: {"other-server" => other_roles}
        )
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions.should be_empty
      end

      it "ignores resource_access when resource_server_id is not set" do
        parser = TokenParserTestHelper.create_token_parser(["sub"])
        roles = LavinMQ::Auth::JWT::ResourceRoles.new(roles: ["read:myvhost/*"])
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user",
          resource_access: {"lavinmq" => roles}
        )
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions.should be_empty
      end
    end

    describe "additional_scopes_key parsing" do
      it "extracts scopes from additional_scopes_key string claim" do
        parser = TokenParserTestHelper.create_token_parser(["sub"], additional_scopes_key: "permissions")
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user"
        )
        payload["permissions"] = JSON::Any.new("read:myvhost/*")
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions["myvhost"][:read].should eq(/.*/)
      end

      it "extracts scopes from additional_scopes_key array claim" do
        parser = TokenParserTestHelper.create_token_parser(["sub"], additional_scopes_key: "permissions")
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user"
        )
        payload["permissions"] = JSON.parse(%(["read:myvhost/*", "write:myvhost/*"]))
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions["myvhost"][:read].should eq(/.*/)
        claims.permissions["myvhost"][:write].should eq(/.*/)
      end

      it "extracts scopes from additional_scopes_key hash claim with resource_server_id" do
        parser = TokenParserTestHelper.create_token_parser(
          ["sub"],
          resource_server_id: "lavinmq",
          additional_scopes_key: "permissions"
        )
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user"
        )
        payload["permissions"] = JSON.parse(%({"lavinmq": ["lavinmq.read:myvhost/*"]}))
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions["myvhost"][:read].should eq(/.*/)
      end

      it "extracts scopes from hash claim without resource_server_id" do
        parser = TokenParserTestHelper.create_token_parser(
          ["sub"],
          additional_scopes_key: "permissions"
        )
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user"
        )
        payload["permissions"] = JSON.parse(%({"anykey": ["read:myvhost/*"]}))
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions["myvhost"][:read].should eq(/.*/)
      end

      it "ignores non-matching resource_server_id in hash claim" do
        parser = TokenParserTestHelper.create_token_parser(
          ["sub"],
          resource_server_id: "lavinmq",
          additional_scopes_key: "permissions"
        )
        payload = LavinMQ::Auth::JWT::Payload.new(
          exp: RoughTime.utc.to_unix + 3600,
          sub: "user"
        )
        payload["permissions"] = JSON.parse(%({"other-server": ["read:myvhost/*"]}))
        token = TokenParserTestHelper.create_mock_token(payload)
        claims = parser.parse(token)
        claims.permissions.should be_empty
      end
    end
  end

  describe "scope prefix filtering" do
    it "filters scopes by explicit prefix" do
      parser = TokenParserTestHelper.create_token_parser(["sub"], scope_prefix: "lavinmq.")
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "lavinmq.read:myvhost/* other.read:other/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:read].should eq(/.*/)
      claims.permissions.has_key?("other").should be_false
    end

    it "uses resource_server_id as default prefix" do
      parser = TokenParserTestHelper.create_token_parser(["sub"], resource_server_id: "lavinmq")
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "lavinmq.read:myvhost/* other.read:other/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:read].should eq(/.*/)
      claims.permissions.has_key?("other").should be_false
    end

    it "does not filter when no prefix is configured" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "read:vhost1/* write:vhost2/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["vhost1"][:read].should eq(/.*/)
      claims.permissions["vhost2"][:write].should eq(/.*/)
    end
  end

  describe "tag parsing" do
    it "parses administrator tag" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "tag:administrator"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.tags.should contain(LavinMQ::Tag::Administrator)
    end

    it "parses monitoring tag" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "tag:monitoring"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.tags.should contain(LavinMQ::Tag::Monitoring)
    end

    it "parses management tag" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "tag:management"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.tags.should contain(LavinMQ::Tag::Management)
    end

    it "parses policymaker tag" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "tag:policymaker"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.tags.should contain(LavinMQ::Tag::PolicyMaker)
    end

    it "parses impersonator tag" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "tag:impersonator"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.tags.should contain(LavinMQ::Tag::Impersonator)
    end

    it "ignores invalid tag names" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "tag:invalid tag:administrator"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.tags.size.should eq(1)
      claims.tags.should contain(LavinMQ::Tag::Administrator)
    end

    it "parses multiple tags" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "tag:administrator tag:monitoring tag:management"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.tags.should contain(LavinMQ::Tag::Administrator)
      claims.tags.should contain(LavinMQ::Tag::Monitoring)
      claims.tags.should contain(LavinMQ::Tag::Management)
    end
  end

  describe "permission parsing" do
    it "parses read permission with colon separator" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "read:myvhost/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:read].should eq(/.*/)
    end

    it "parses write permission" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "write:myvhost/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:write].should eq(/.*/)
    end

    it "parses configure permission" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "configure:myvhost/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:config].should eq(/.*/)
    end

    it "parses configure permission with different separators" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "configure:myvhost/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:config].should eq(/.*/)
    end

    it "converts wildcard * to .* pattern" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "read:myvhost/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:read].should eq(/.*/)
    end

    it "parses permissions with dot separator" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "read:myvhost/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:read].should eq(/.*/)
    end

    it "parses permissions with slash separator" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "read:myvhost/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:read].should eq(/.*/)
    end

    it "parses multiple permissions for same vhost" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "read:myvhost/* write:myvhost/queue configure:myvhost/^$"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:read].should eq(/.*/)
      claims.permissions["myvhost"][:write].should eq(/^queue$/)
      claims.permissions["myvhost"][:config].should eq(/^\^\$$/)
    end

    it "parses permissions for multiple vhosts" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "read:vhost1/* read:vhost2/queue"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["vhost1"][:read].should eq(/.*/)
      claims.permissions["vhost2"][:read].should eq(/^queue$/)
    end

    it "ignores scopes with wrong format" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "invalid read:myvhost/* too:many/parts/here"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions.size.should eq(1)
      claims.permissions["myvhost"][:read].should eq(/.*/)
    end

    it "ignores unknown permission types" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "delete:myvhost/* read:myvhost/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:read].should eq(/.*/)
      claims.permissions["myvhost"][:config].should eq(/^$/)
      claims.permissions["myvhost"][:write].should eq(/^$/)
    end

    it "initializes missing permission types to empty pattern" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "read:myvhost/*"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["myvhost"][:config].should eq(/^$/)
      claims.permissions["myvhost"][:write].should eq(/^$/)
    end

    it "skips scopes with invalid regex patterns" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "read:myvhost/[invalid read:other/valid"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.permissions["other"][:read].should eq(/^valid$/)
    end
  end

  describe "combined tags and permissions" do
    it "parses both tags and permissions from same scope" do
      parser = TokenParserTestHelper.create_token_parser(["sub"])
      payload = LavinMQ::Auth::JWT::Payload.new(
        exp: RoughTime.utc.to_unix + 3600,
        sub: "user",
        scope: "tag:administrator read:myvhost/* write:myvhost/queue"
      )
      token = TokenParserTestHelper.create_mock_token(payload)
      claims = parser.parse(token)
      claims.tags.should contain(LavinMQ::Tag::Administrator)
      claims.permissions["myvhost"][:read].should eq(/.*/)
      claims.permissions["myvhost"][:write].should eq(/^queue$/)
    end
  end
end
