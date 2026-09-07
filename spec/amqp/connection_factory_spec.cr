require "../spec_helper"
require "../../src/lavinmq/connection_info"
require "../../src/lavinmq/amqp/connection_factory"

# ConnectionFactory#credentials dispatches to the SASL mechanism handlers; the
# per-mechanism parsing is covered in spec/auth/mechanisms/*.
describe LavinMQ::AMQP::ConnectionFactory do
  describe "#credentials" do
    it "dispatches PLAIN to the mechanism and returns its username and password" do
      with_amqp_server do |s|
        factory = LavinMQ::AMQP::ConnectionFactory.new(s.authenticator, s.vhosts)
        ci = LavinMQ::ConnectionInfo.local
        nul = 0.chr
        start_ok = start_ok_frame("PLAIN", "#{nul}user#{nul}pass")
        factory.credentials(start_ok, ci).should eq({"user", "pass"})
      end
    end

    it "dispatches EXTERNAL with the connection's certificate details" do
      with_amqp_server do |s|
        LavinMQ::Config.instance.external_auth_login_from = "common_name"
        factory = LavinMQ::AMQP::ConnectionFactory.new(s.authenticator, s.vhosts)
        ci = LavinMQ::ConnectionInfo.local
        ci.ssl_cn = "anders"
        start_ok = start_ok_frame("EXTERNAL", "")
        factory.credentials(start_ok, ci).should eq({"anders", ""})
      end
    end

    it "raises for an unsupported mechanism" do
      with_amqp_server do |s|
        factory = LavinMQ::AMQP::ConnectionFactory.new(s.authenticator, s.vhosts)
        ci = LavinMQ::ConnectionInfo.local
        start_ok = start_ok_frame("KERBEROS", "")
        expect_raises(Exception, /Unsupported authentication mechanism/) do
          factory.credentials(start_ok, ci)
        end
      end
    end
  end
end

# Builds a Connection::StartOk frame with the given mechanism and response.
def start_ok_frame(mechanism : String, response : String)
  AMQ::Protocol::Frame::Connection::StartOk.new(
    client_properties: AMQ::Protocol::Table.new,
    mechanism: mechanism,
    response: response,
    locale: "en_US")
end
