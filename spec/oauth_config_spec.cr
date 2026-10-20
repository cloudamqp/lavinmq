require "./spec_helper"

private def parse_oauth_config(ini : String) : LavinMQ::Config
  config_file = File.tempfile("lavinmq-config", ".ini", &.print(ini))
  config = LavinMQ::Config.new(IO::Memory.new)
  config.parse(["-c", config_file.path])
  config
ensure
  config_file.try &.delete
  Log.setup(:fatal)
end

describe LavinMQ::Config do
  describe "OAuth configuration validation" do
    it "sets default oauth_verify_aud to true" do
      config = LavinMQ::Config.new
      config.oauth_verify_aud?.should be_true
    end

    it "sets default oauth_jwks_cache_ttl to 1 hour" do
      config = LavinMQ::Config.new
      config.oauth_jwks_cache_ttl.should eq(1.hours)
    end

    it "sets default oauth_preferred_username_claims to sub and client_id" do
      config = LavinMQ::Config.new
      config.oauth_preferred_username_claims.should eq(["sub", "client_id"])
    end

    it "sets default oauth_additional_scopes_keys to empty array" do
      config = LavinMQ::Config.new
      config.oauth_additional_scopes_keys.should be_empty
    end

    it "sets default oauth_scope_prefix to nil" do
      config = LavinMQ::Config.new
      config.oauth_scope_prefix.should be_nil
    end

    it "sets default oauth_audience to nil" do
      config = LavinMQ::Config.new
      config.oauth_audience.should be_nil
    end

    it "sets default oauth_resource_server_id to nil" do
      config = LavinMQ::Config.new
      config.oauth_resource_server_id.should be_nil
    end

    it "sets default oauth_mgmt_scopes to \"openid profile\"" do
      config = LavinMQ::Config.new
      config.oauth_mgmt_scopes.should eq("openid profile")
    end
  end

  describe "#oauth_providers" do
    it "is empty when no issuer is configured" do
      LavinMQ::Config.new.oauth_providers.should be_empty
    end

    it "carries the [oauth] settings" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          resource_server_id = my-service
          preferred_username_claims = email
          additional_scopes_keys = roles
          scope_prefix = mq.
          verify_aud = false
          audience = my-audience
          jwks_cache_ttl = 300
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      provider = config.oauth_providers.first
      provider.issuer_url.should eq(URI.parse("https://auth.example.com"))
      provider.resource_server_id.should eq("my-service")
      provider.preferred_username_claims.should eq(["email"])
      provider.additional_scopes_keys.should eq(["roles"])
      provider.scope_prefix.should eq("mq.")
      provider.verify_aud?.should be_false
      provider.audience.should eq("my-audience")
      provider.jwks_cache_ttl.should eq(300.seconds)
    end
  end

  describe "[oauth.<name>] sections" do
    it "parses each section into a named provider" do
      config = parse_oauth_config(<<-CONFIG)
        [oauth.keycloak]
        issuer = https://keycloak.example.com/realms/prod
        resource_server_id = lavinmq

        [oauth.entra]
        issuer = https://login.microsoftonline.com/tenant/v2.0
        audience = api://lavinmq
        additional_scopes_keys = roles
        scope_prefix = lavinmq.
        preferred_username_claims = preferred_username, sub
        verify_aud = false
        jwks_cache_ttl = 600
        CONFIG
      keycloak, entra = config.oauth_providers
      keycloak.name.should eq "keycloak"
      keycloak.issuer_url.should eq URI.parse("https://keycloak.example.com/realms/prod")
      keycloak.resource_server_id.should eq "lavinmq"
      keycloak.audience.should be_nil
      keycloak.verify_aud?.should be_true
      keycloak.jwks_cache_ttl.should eq 1.hour
      entra.name.should eq "entra"
      entra.audience.should eq "api://lavinmq"
      entra.additional_scopes_keys.should eq ["roles"]
      entra.scope_prefix.should eq "lavinmq."
      entra.preferred_username_claims.should eq ["preferred_username", "sub"]
      entra.verify_aud?.should be_false
      entra.jwks_cache_ttl.should eq 600.seconds
    end

    it "lists the [oauth] provider first, named default" do
      config = parse_oauth_config(<<-CONFIG)
        [oauth.entra]
        issuer = https://login.microsoftonline.com/tenant/v2.0

        [oauth]
        issuer = https://keycloak.example.com/realms/prod
        CONFIG
      config.oauth_providers.map(&.name).should eq ["default", "entra"]
    end

    it "doesn't share provider objects between calls" do
      config = parse_oauth_config("[oauth.a]\nissuer = https://a.example.com\n")
      config.oauth_providers.first.audience = "changed"
      config.oauth_providers.first.audience.should be_nil
    end

    it "rejects a section without an issuer" do
      expect_raises(LavinMQ::Config::Error, /Missing issuer in \[oauth.entra\]/) do
        parse_oauth_config("[oauth.entra]\naudience = api://lavinmq\n")
      end
    end

    it "rejects an invalid provider name" do
      expect_raises(LavinMQ::Config::Error, /Invalid OAuth provider name/) do
        parse_oauth_config("[oauth.my idp]\nissuer = https://a.example.com\n")
      end
    end

    it "rejects an invalid value" do
      expect_raises(LavinMQ::Config::Error, /'jwks_cache_ttl' in \[oauth.a\]/) do
        parse_oauth_config("[oauth.a]\nissuer = https://a.example.com\njwks_cache_ttl = soon\n")
      end
    end

    it "rejects two providers with the same issuer" do
      expect_raises(LavinMQ::Config::Error, /'a' and 'b' have the same issuer/) do
        parse_oauth_config(<<-CONFIG)
          [oauth.a]
          issuer = https://idp.example.com
          [oauth.b]
          issuer = https://idp.example.com/
          CONFIG
      end
    end

    it "rejects [oauth.default] together with an [oauth] issuer" do
      expect_raises(LavinMQ::Config::Error, /'default' is configured more than once/) do
        parse_oauth_config(<<-CONFIG)
          [oauth]
          issuer = https://a.example.com
          [oauth.default]
          issuer = https://b.example.com
          CONFIG
      end
    end

    it "drops sections removed from the file on reload" do
      config_file = File.tempfile("lavinmq-config", ".ini")
      begin
        File.write(config_file.path, "[oauth.a]\nissuer = https://a.example.com\n[oauth.b]\nissuer = https://b.example.com\n")
        config = LavinMQ::Config.new(IO::Memory.new)
        config.parse(["-c", config_file.path])
        config.oauth_providers.map(&.name).should eq ["a", "b"]

        File.write(config_file.path, "[oauth.b]\nissuer = https://b.example.com\naudience = new-api\n")
        config.reload
        config.oauth_providers.map(&.name).should eq ["b"]
        config.oauth_providers.first.audience.should eq "new-api"
      ensure
        File.delete?(config_file.path)
        Log.setup(:fatal)
      end
    end
  end

  describe "OAuth configuration parsing" do
    it "parses oauth section from config with issuer_url" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_issuer_url.should eq(URI.parse("https://auth.example.com"))
    end

    it "parses resource_server_id" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          resource_server_id = my-service
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_resource_server_id.should eq("my-service")
    end

    it "parses preferred_username_claims as comma-separated list" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          preferred_username_claims = email, preferred_username, sub
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_preferred_username_claims.should eq(["email", "preferred_username", "sub"])
    end

    it "parses additional_scopes_keys" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          additional_scopes_keys = custom_permissions
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_additional_scopes_keys.should eq(["custom_permissions"])
    end

    it "parses multiple comma-separated additional_scopes_keys" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          additional_scopes_keys = roles, permissions
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_additional_scopes_keys.should eq(["roles", "permissions"])
    end

    it "parses scope_prefix" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          scope_prefix = mq.
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_scope_prefix.should eq("mq.")
    end

    it "parses mgmt_scopes" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          mgmt_scopes = openid email offline_access
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_mgmt_scopes.should eq("openid email offline_access")
    end

    it "parses verify_aud as boolean" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          verify_aud = false
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_verify_aud?.should be_false
    end

    it "parses audience" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          audience = lavinmq-api
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_audience.should eq("lavinmq-api")
    end

    it "parses jwks_cache_ttl as seconds" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
          [oauth]
          issuer = https://auth.example.com
          jwks_cache_ttl = 7200
          CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_jwks_cache_ttl.should eq(7200.seconds)
    end
  end

  describe "#oauth_mgmt_ui_enabled?" do
    cases = {
      "https://mq.example.com"     => true,
      "http://localhost:15672"     => true,
      "http://127.0.0.1:15672"     => true,
      "http://[::1]:15672"         => true,
      "http://localhost.evil.com/" => false,
      "http://127.0.0.1.evil.com/" => false,
      "http://example.com"         => false,
      "http://localhostfoo"        => false,
    }

    cases.each do |base_url, expected|
      it "returns #{expected} for #{base_url}" do
        config = LavinMQ::Config.new
        config.oauth_client_id = "test-client"
        config.oauth_issuer_url = URI.parse("https://idp.example.com")
        config.oauth_mgmt_base_url = URI.parse(base_url)
        config.oauth_mgmt_ui_enabled?.should eq(expected)
      end
    end

    it "returns false when oauth_client_id is missing" do
      config = LavinMQ::Config.new
      config.oauth_issuer_url = URI.parse("https://idp.example.com")
      config.oauth_mgmt_base_url = URI.parse("https://mq.example.com")
      config.oauth_mgmt_ui_enabled?.should be_false
    end

    it "returns false when oauth_issuer_url is missing" do
      config = LavinMQ::Config.new
      config.oauth_client_id = "test-client"
      config.oauth_mgmt_base_url = URI.parse("https://mq.example.com")
      config.oauth_mgmt_ui_enabled?.should be_false
    end
  end
end
