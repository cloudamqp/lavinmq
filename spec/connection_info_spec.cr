require "spec"
require "base64"
require "openssl"
require "../src/lavinmq/connection_info"

describe LavinMQ::ConnectionInfo::IPAddress do
  describe "#address" do
    it "unmaps IPv4-mapped IPv6 addresses" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::ffff:127.0.0.1", 1234))

      addr.address.should eq "127.0.0.1"
      addr.to_s.should eq "127.0.0.1:1234"
    end

    it "preserves plain IPv4 addresses" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("10.1.2.3", 1234))

      addr.address.should eq "10.1.2.3"
    end

    it "preserves native IPv6 addresses" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::1", 1234))

      addr.address.should eq "::1"
    end
  end

  describe "#loopback?" do
    it "recognizes IPv4-mapped IPv6 loopback as loopback" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::ffff:127.0.0.1", 0))
      addr.loopback?.should be_true
    end
  end
end

# SSL certificate extraction used by the SASL EXTERNAL auth mechanism.
# The extraction methods take a peer certificate directly, so they can be tested
# against a loaded certificate without a live TLS handshake. The socket wiring in
# #with_ssl is exercised end-to-end in mtls_spec and external_auth_e2e_spec.
describe LavinMQ::ConnectionInfo do
  ci = LavinMQ::ConnectionInfo.local
  client_cert = load_certificate("spec/resources/client_certificate.pem")

  describe "#extract_common_name" do
    it "returns the certificate common name" do
      ci.extract_common_name(client_cert).should eq "anders"
    end

    it "returns nil without a peer certificate" do
      ci.extract_common_name(nil).should be_nil
    end
  end

  describe "#extract_subject_alternative_name_entries" do
    it "returns nil without a peer certificate" do
      ci.extract_subject_alternative_name_entries(nil).should be_nil
    end

    it "extracts the SAN values" do
      entries = ci.extract_subject_alternative_name_entries(client_cert).not_nil!
      entries.map(&.value).should contain "localhost"
    end

    it "extracts the type of each entry" do
      # The test client certificate only carries DNS SANs.
      entries = ci.extract_subject_alternative_name_entries(client_cert).not_nil!
      entries.should_not be_empty
      entries.map(&.type).uniq!.should eq ["DNS"]
    end

    it "indexes entries in order" do
      entries = ci.extract_subject_alternative_name_entries(client_cert).not_nil!
      entries.map(&.index).should eq (0...entries.size).to_a
    end
  end
end

# Loads an X509 certificate from a PEM file without needing a TLS handshake.
def load_certificate(path : String) : OpenSSL::X509::Certificate
  pem = File.read(path)
  body = pem.each_line.reject(&.starts_with?("-----")).join
  cert, _ = OpenSSL::X509::Certificate.from_der?(Base64.decode(body))
  cert || raise "could not load certificate from #{path}"
end
