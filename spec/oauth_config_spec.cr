require "./spec_helper"

describe LavinMQ::Config do
  describe "OAuth configuration validation" do
    it "accepts valid HTTPS issuer URL when OAuth is enabled" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = "https://auth.example.com"
      config.validate_oauth_config
      # Should not raise
    end

    it "accepts HTTPS issuer URL with port" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = "https://auth.example.com:8443"
      config.validate_oauth_config
      # Should not raise
    end

    it "accepts HTTPS issuer URL with path" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = "https://auth.example.com/realms/master"
      config.validate_oauth_config
      # Should not raise
    end

    it "rejects HTTP (non-HTTPS) issuer URL when OAuth is enabled" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = "http://auth.example.com"

      expect_raises(ArgumentError, /must use HTTPS scheme/) do
        config.validate_oauth_config
      end
    end

    it "rejects issuer URL without scheme when OAuth is enabled" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = "auth.example.com"

      expect_raises(ArgumentError, /must use HTTPS scheme/) do
        config.validate_oauth_config
      end
    end

    it "rejects issuer URL with missing host when OAuth is enabled" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = "https://"

      expect_raises(ArgumentError, /must have a valid host/) do
        config.validate_oauth_config
      end
    end

    it "rejects malformed issuer URL when OAuth is enabled" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = "not a valid url at all"

      # Malformed URLs get caught and return nil scheme, triggering the HTTPS check first
      expect_raises(ArgumentError) do
        config.validate_oauth_config
      end
    end

    it "skips validation when OAuth is not in auth_backends" do
      config = LavinMQ::Config.new
      config.auth_backends = ["local"]
      config.oauth_issuer_url = "http://bad-url"
      config.validate_oauth_config
      # Should not raise (OAuth is not enabled)
    end

    it "requires issuer URL when OAuth is enabled" do
      config = LavinMQ::Config.new
      config.auth_backends = ["oauth"]
      config.oauth_issuer_url = ""

      expect_raises(ArgumentError, /oauth_issuer_url must be configured/) do
        config.validate_oauth_config
      end
    end

    it "sets default oauth_verify_aud to true" do
      config = LavinMQ::Config.new
      config.oauth_verify_aud?.should be_true
    end

    it "sets default oauth_jwks_cache_ttl to 1 hour" do
      config = LavinMQ::Config.new
      config.oauth_jwks_cache_ttl.should eq(1.hours)
    end

    it "sets default oauth_preferred_username_claims to empty array" do
      config = LavinMQ::Config.new
      config.oauth_preferred_username_claims.should be_empty
    end

    it "sets default oauth_additional_scopes_key to empty string" do
      config = LavinMQ::Config.new
      config.oauth_additional_scopes_key.should eq("")
    end

    it "sets default oauth_scope_prefix to empty string" do
      config = LavinMQ::Config.new
      config.oauth_scope_prefix.should eq("")
    end

    it "sets default oauth_audience to empty string" do
      config = LavinMQ::Config.new
      config.oauth_audience.should eq("")
    end

    it "sets default oauth_resource_server_id to empty string" do
      config = LavinMQ::Config.new
      config.oauth_resource_server_id.should eq("")
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
      config.config_file = config_file.path
      config.parse
      config.oauth_issuer_url.should eq("https://auth.example.com")
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
      config.config_file = config_file.path
      config.parse
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
      config.config_file = config_file.path
      config.parse
      config.oauth_preferred_username_claims.should eq(["email", "preferred_username", "sub"])
    end

    it "parses additional_scopes_key" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
        [oauth]
        issuer = https://auth.example.com
        additional_scopes_key = custom_permissions
        CONFIG
      end

      config = LavinMQ::Config.new
      config.config_file = config_file.path
      config.parse
      config.oauth_additional_scopes_key.should eq("custom_permissions")
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
      config.config_file = config_file.path
      config.parse
      config.oauth_scope_prefix.should eq("mq.")
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
      config.config_file = config_file.path
      config.parse
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
      config.config_file = config_file.path
      config.parse
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
      config.config_file = config_file.path
      config.parse
      config.oauth_jwks_cache_ttl.should eq(7200.seconds)
    end

    it "validates oauth config after parsing when OAuth is enabled" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
        [main]
        auth_backends = oauth
        [oauth]
        issuer = http://auth.example.com
        CONFIG
      end

      config = LavinMQ::Config.new
      config.config_file = config_file.path

      # Config.parse calls abort on error, which raises SpecExit in tests
      expect_raises(SpecExit) do
        config.parse
      end
    end
  end
end
