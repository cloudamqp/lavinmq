require "spec"
require "../../../src/lavinmq/auth/mechanisms/amqplain"

describe LavinMQ::Auth::Mechanisms::AMQPlain do
  ci = LavinMQ::ConnectionInfo.local
  mechanism = LavinMQ::Auth::Mechanisms::AMQPlain.new

  it "extracts LOGIN and PASSWORD from the field table" do
    response = amqplain_response({"LOGIN" => "user", "PASSWORD" => "pass"})
    mechanism.credentials(response, ci).should eq({"user", "pass"})
  end

  it "defaults to empty strings when fields are missing" do
    response = amqplain_response({"OTHER" => "x"})
    mechanism.credentials(response, ci).should eq({"", ""})
  end
end

# The AMQPLAIN response is a bare AMQP field table body (no length prefix).
def amqplain_response(hash) : String
  String.new(AMQ::Protocol::Table.new(hash).to_slice)
end
