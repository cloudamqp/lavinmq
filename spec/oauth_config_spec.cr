require "./spec_helper"

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

    it "sets default oauth_preferred_username_claims to empty array" do
      config = LavinMQ::Config.new
      config.oauth_preferred_username_claims.should be_empty
    end

    it "sets default oauth_additional_scopes_key to nil" do
      config = LavinMQ::Config.new
      config.oauth_additional_scopes_key.should be_nil
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
        issuer_url = https://auth.example.com
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
        issuer_url = https://auth.example.com
        preferred_username_claims = email, preferred_username, sub
        CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_preferred_username_claims.should eq(["email", "preferred_username", "sub"])
    end

    it "parses additional_scopes_key" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
        [oauth]
        issuer_url = https://auth.example.com
        additional_scopes_key = custom_permissions
        CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_additional_scopes_key.should eq("custom_permissions")
    end

    it "parses scope_prefix" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
        [oauth]
        issuer_url = https://auth.example.com
        scope_prefix = mq.
        CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_scope_prefix.should eq("mq.")
    end

    it "parses verify_aud as boolean" do
      config_file = File.tempfile do |file|
        file.print <<-CONFIG
        [oauth]
        issuer_url = https://auth.example.com
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
        issuer_url = https://auth.example.com
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
        issuer_url = https://auth.example.com
        jwks_cache_ttl = 7200
        CONFIG
      end

      config = LavinMQ::Config.new
      config.parse(["-c", config_file.path])
      config.oauth_jwks_cache_ttl.should eq(7200.seconds)
    end
  end
end
