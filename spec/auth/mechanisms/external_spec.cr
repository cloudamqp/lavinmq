require "spec"
require "../../../src/lavinmq/auth/mechanisms/external"

describe LavinMQ::Auth::Mechanisms::External do
  mechanism = LavinMQ::Auth::Mechanisms::External.new

  # Reset the shared config so each test starts unconfigured, and later specs
  # don't see EXTERNAL enabled.
  before_each { reset_external_auth_config }
  after_each { reset_external_auth_config }

  it "raises when EXTERNAL is not configured" do
    ci = verified_connection_info
    ci.ssl_cn = "anders"
    expect_raises(LavinMQ::Auth::Mechanisms::Error, /not enabled/) do
      mechanism.credentials("", ci)
    end
  end

  it "raises when the client certificate wasn't verified" do
    LavinMQ::Config.instance.external_auth_login_from = "common_name"
    ci = LavinMQ::ConnectionInfo.local
    ci.ssl = true
    ci.ssl_cn = "anders"
    expect_raises(LavinMQ::Auth::Mechanisms::Error, /verified TLS client certificate/) do
      mechanism.credentials("", ci)
    end
  end

  it "raises for certificate details from a PROXY protocol header" do
    LavinMQ::Config.instance.external_auth_login_from = "common_name"
    addr = Socket::IPAddress.new("127.0.0.1", 0)
    ci = LavinMQ::ConnectionInfo.new(addr, addr, proxied: true)
    ci.ssl = true
    ci.ssl_verify = true
    ci.ssl_cn = "anders"
    expect_raises(LavinMQ::Auth::Mechanisms::Error, /verified TLS client certificate/) do
      mechanism.credentials("", ci)
    end
  end

  it "accepts a proxied connection where LavinMQ terminated TLS" do
    LavinMQ::Config.instance.external_auth_login_from = "common_name"
    addr = Socket::IPAddress.new("127.0.0.1", 0)
    ci = LavinMQ::ConnectionInfo.new(addr, addr, proxied: true)
    ci.ssl = true
    ci.tls_terminated = true
    ci.ssl_verify = true
    ci.ssl_cn = "anders"
    mechanism.credentials("", ci).should eq({"anders", ""})
  end

  describe ".available?" do
    it "is true for a verified client certificate when configured" do
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      LavinMQ::Auth::Mechanisms::External.available?(verified_connection_info).should be_true
    end

    it "is false when not configured" do
      LavinMQ::Auth::Mechanisms::External.available?(verified_connection_info).should be_false
    end

    it "is false without a verified client certificate" do
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      LavinMQ::Auth::Mechanisms::External.available?(LavinMQ::ConnectionInfo.local).should be_false
    end
  end

  describe "common_name" do
    it "derives the username from the certificate common name" do
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      ci = verified_connection_info
      ci.ssl_cn = "anders"
      mechanism.credentials("", ci).should eq({"anders", ""})
    end

    it "raises when no common name is present" do
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      ci = verified_connection_info
      expect_raises(LavinMQ::Auth::Mechanisms::Error, /no Common Name/) do
        mechanism.credentials("", ci)
      end
    end
  end

  describe "subject_alternative_name" do
    fixture = [
      LavinMQ::ConnectionInfo::SubjectAlternativeName.new("DNS", "anders"),
      LavinMQ::ConnectionInfo::SubjectAlternativeName.new("email", "anders@example.com"),
      LavinMQ::ConnectionInfo::SubjectAlternativeName.new("DNS", "localhost"),
    ]

    it "selects the first entry when no type is configured" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_index = 0
      ci = verified_connection_info
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"anders", ""})
    end

    it "defaults to the first entry when no index is configured" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      ci = verified_connection_info
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"anders", ""})
    end

    it "filters by SAN type and selects by index within the matches" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      LavinMQ::Config.instance.external_auth_san_index = 1
      ci = verified_connection_info
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"localhost", ""})
    end

    it "matches the SAN type case-insensitively" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "dns"
      LavinMQ::Config.instance.external_auth_san_index = 0
      ci = verified_connection_info
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"anders", ""})
    end

    it "selects an entry of a non-DNS type" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "email"
      LavinMQ::Config.instance.external_auth_san_index = 0
      ci = verified_connection_info
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"anders@example.com", ""})
    end

    it "raises when no entry matches the configured type" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "URI"
      ci = verified_connection_info
      ci.ssl_san_entries = fixture
      expect_raises(LavinMQ::Auth::Mechanisms::Error, /no Subject Alternative Name of type/) do
        mechanism.credentials("", ci)
      end
    end

    it "raises when the index is out of range for the type" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      LavinMQ::Config.instance.external_auth_san_index = 5
      ci = verified_connection_info
      ci.ssl_san_entries = fixture
      expect_raises(LavinMQ::Auth::Mechanisms::Error, /no Subject Alternative Name of type/) do
        mechanism.credentials("", ci)
      end
    end

    it "raises for a negative index" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_index = -1
      ci = verified_connection_info
      ci.ssl_san_entries = fixture
      expect_raises(LavinMQ::Auth::Mechanisms::Error, /no Subject Alternative Name of type/) do
        mechanism.credentials("", ci)
      end
    end

    it "raises when the certificate has no SAN entries" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      ci = verified_connection_info
      expect_raises(LavinMQ::Auth::Mechanisms::Error, /no Subject Alternative Name$/) do
        mechanism.credentials("", ci)
      end
    end
  end
end

# A connection whose client certificate LavinMQ verified in the TLS handshake.
def verified_connection_info : LavinMQ::ConnectionInfo
  ci = LavinMQ::ConnectionInfo.local
  ci.ssl = true
  ci.tls_terminated = true
  ci.ssl_verify = true
  ci
end

def reset_external_auth_config
  c = LavinMQ::Config.instance
  c.external_auth_login_from = nil
  c.external_auth_san_type = nil
  c.external_auth_san_index = nil
end
