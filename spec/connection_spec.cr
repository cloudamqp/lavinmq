require "./spec_helper"
require "../src/avalanchemq/connection"

class TestConnection < AvalancheMQ::Connection
  def read_loop
    spawn do
      loop do
        AvalancheMQ::AMQP::Frame.decode(@socket) do |frame|
          case frame
          when AvalancheMQ::AMQP::BodyFrame
            frame.body.skip(frame.body_size)
            true
          when AvalancheMQ::AMQP::Connection::CloseOk
            @socket.close
            false
          else true
          end
        end || break
      end
    end
  end

  def cleanup
    close
    @socket.close
  end
end

describe AvalancheMQ::Connection do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL

  it "should connect" do
    conn = TestConnection.new(URI.parse("amqp://localhost"), log)
    conn.closed?.should be_false
  ensure
    conn.try &.close
  end

  it "should close" do
    conn = TestConnection.new(URI.parse("amqp://localhost"), log)
    conn.read_loop
    Fiber.yield
    conn.close
    sleep 0.05
    conn.closed?.should be_true
  ensure
    conn.try &.cleanup
  end

  it "should support heartbeat query param" do
    conn = TestConnection.new(URI.parse("amqp://localhost?heartbeat=19"), log)
    Fiber.yield
    s.connections.last.as(AvalancheMQ::NetworkClient).heartbeat.should eq 19
  ensure
    conn.try &.close
  end

  it "should support channel_max query param" do
    conn = TestConnection.new(URI.parse("amqp://localhost?channel_max=19"), log)
    Fiber.yield
    s.connections.last.as(AvalancheMQ::NetworkClient).channel_max.should eq 19
  ensure
    conn.try &.close
  end

  it "should support auth_mechanism query param" do
    conn = TestConnection.new(URI.parse("amqp://localhost?auth_mechanism=AMQPLAIN"), log)
    Fiber.yield
    s.connections.last.as(AvalancheMQ::NetworkClient).auth_mechanism.should eq "AMQPLAIN"
  ensure
    conn.try &.close
  end

  it "should support amqps verify=none" do
    conn = TestConnection.new(URI.parse("amqps://localhost?verify=none"), log)
    Fiber.yield
    conn.verify_mode.should eq OpenSSL::SSL::VerifyMode::NONE
  ensure
    conn.try &.close
  end

  it "should support amqps certfile/keyfile" do
    cert = Dir.current + "/spec/resources/client_certificate.pem"
    key = Dir.current + "/spec/resources/client_key.pem"
    ca = Dir.current + "/spec/resources/ca_certificate.pem"
    uri = URI.parse("amqps://localhost?certfile=#{cert}&keyfile=#{key}&cacertfile=#{ca}")
    conn = TestConnection.new(uri, log)
    Fiber.yield
    s.connections.empty?.should be_false
  ensure
    conn.try &.close
  end
end
