require "./spec_helper"
require "../src/avalanchemq/connection"

class TestConnection < AvalancheMQ::Connection
  def initialize(uri : String)
    super(URI.parse(uri))
  end

  def read_loop
    spawn do
      loop do
        AvalancheMQ::AMQP::Frame.from_io(@socket) do |frame|
          case frame
          when AvalancheMQ::AMQP::Frame::Body
            frame.body.skip(frame.body_size)
            true
          when AvalancheMQ::AMQP::Frame::Connection::CloseOk
            @socket.close
            false
          else true
          end
        end || break
      end
    rescue IO::Error
    end
  end

  def cleanup
    @socket.close unless @socket.closed?
  end
end

describe AvalancheMQ::Connection do
  it "should connect" do
    conn = TestConnection.new(AMQP_BASE_URL)
    conn.closed?.should be_false
  ensure
    conn.try &.close
  end

  it "should close" do
    conn = TestConnection.new(AMQP_BASE_URL)
    conn.read_loop
    Fiber.yield
    conn.close
    sleep 0.05
    conn.closed?.should be_true
  ensure
    conn.try &.cleanup
  end

  it "should support heartbeat query param" do
    conn = TestConnection.new("#{AMQP_BASE_URL}?heartbeat=19")
    Fiber.yield
    s.connections.last.as(AvalancheMQ::NetworkClient).heartbeat.should eq 19
  ensure
    conn.try &.close
  end

  it "should support channel_max query param" do
    conn = TestConnection.new("#{AMQP_BASE_URL}?channel_max=19")
    Fiber.yield
    s.connections.last.as(AvalancheMQ::NetworkClient).channel_max.should eq 19
  ensure
    conn.try &.close
  end

  it "should support auth_mechanism query param" do
    conn = TestConnection.new("#{AMQP_BASE_URL}?auth_mechanism=AMQPLAIN")
    Fiber.yield
    s.connections.last.as(AvalancheMQ::NetworkClient).auth_mechanism.should eq "AMQPLAIN"
  ensure
    conn.try &.close
  end

  it "should support amqps verify=none" do
    conn = TestConnection.new("#{AMQPS_BASE_URL}?verify=none")
    Fiber.yield
    conn.verify_mode.should eq OpenSSL::SSL::VerifyMode::NONE
  ensure
    conn.try &.close
  end

  it "should support amqps certfile/keyfile" do
    cert = Dir.current + "/spec/resources/client_certificate.pem"
    key = Dir.current + "/spec/resources/client_key.pem"
    ca = Dir.current + "/spec/resources/ca_certificate.pem"
    uri = "#{AMQP_BASE_URL}?certfile=#{cert}&keyfile=#{key}&cacertfile=#{ca}"
    conn = TestConnection.new(uri)
    Fiber.yield
    s.connections.empty?.should be_false
  ensure
    conn.try &.close
  end
end
