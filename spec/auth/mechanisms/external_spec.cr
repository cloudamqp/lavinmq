require "spec"
require "../../../src/lavinmq/auth/mechanisms/external"

describe LavinMQ::Auth::Mechanisms::External do
  mechanism = LavinMQ::Auth::Mechanisms::External.new

  # Reset the shared config between examples so each test starts unconfigured.
  before_each do
    c = LavinMQ::Config.instance
    c.external_auth_login_from = nil
    c.external_auth_san_type = nil
    c.external_auth_san_index = nil
  end

  it "raises when EXTERNAL is not configured" do
    ci = LavinMQ::ConnectionInfo.local
    ci.ssl_cn = "anders"
    expect_raises(Exception, /EXTERNAL is not configured/) do
      mechanism.credentials("", ci)
    end
  end

  describe "common_name" do
    it "derives the username from the certificate common name" do
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      ci = LavinMQ::ConnectionInfo.local
      ci.ssl_cn = "anders"
      mechanism.credentials("", ci).should eq({"anders", ""})
    end

    it "raises when no common name is present" do
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      ci = LavinMQ::ConnectionInfo.local
      expect_raises(Exception, /no SSL Common Name/) do
        mechanism.credentials("", ci)
      end
    end
  end

  describe "subject_alternative_name" do
    fixture = [
      LavinMQ::ConnectionInfo::SubjectAlternativeName.new(0, "DNS", "anders"),
      LavinMQ::ConnectionInfo::SubjectAlternativeName.new(1, "email", "anders@example.com"),
      LavinMQ::ConnectionInfo::SubjectAlternativeName.new(2, "DNS", "localhost"),
    ]

    it "selects the first entry when no type is configured" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_index = 0
      ci = LavinMQ::ConnectionInfo.local
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"anders", ""})
    end

    it "defaults to the first entry when no index is configured" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      ci = LavinMQ::ConnectionInfo.local
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"anders", ""})
    end

    it "filters by SAN type and selects by index within the matches" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      LavinMQ::Config.instance.external_auth_san_index = 1
      ci = LavinMQ::ConnectionInfo.local
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"localhost", ""})
    end

    it "matches the SAN type case-insensitively" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "dns"
      LavinMQ::Config.instance.external_auth_san_index = 0
      ci = LavinMQ::ConnectionInfo.local
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"anders", ""})
    end

    it "selects an entry of a non-DNS type" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "email"
      LavinMQ::Config.instance.external_auth_san_index = 0
      ci = LavinMQ::ConnectionInfo.local
      ci.ssl_san_entries = fixture
      mechanism.credentials("", ci).should eq({"anders@example.com", ""})
    end

    it "raises when no entry matches the configured type" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "URI"
      ci = LavinMQ::ConnectionInfo.local
      ci.ssl_san_entries = fixture
      expect_raises(Exception, /missing SAN/) do
        mechanism.credentials("", ci)
      end
    end

    it "raises when the index is out of range for the type" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      LavinMQ::Config.instance.external_auth_san_index = 5
      ci = LavinMQ::ConnectionInfo.local
      ci.ssl_san_entries = fixture
      expect_raises(Exception, /missing SAN/) do
        mechanism.credentials("", ci)
      end
    end

    it "raises when the certificate has no SAN entries" do
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      ci = LavinMQ::ConnectionInfo.local
      expect_raises(Exception, /no SAN found/) do
        mechanism.credentials("", ci)
      end
    end
  end
end
