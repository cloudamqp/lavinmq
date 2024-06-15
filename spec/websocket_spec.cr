require "./spec_helper"

describe "Websocket support" do
  it "should connect over websocket" do
    with_http_server do |http, _|
      c = AMQP::Client.new("ws://#{http.addr}")
      conn = c.connect
      conn.should_not be_nil
      conn.close
    end
  end

  it "can publish large messages" do
    with_http_server do |http, _|
      c = AMQP::Client.new("ws://#{http.addr}")
      conn = c.connect
      ch = conn.channel
      q = ch.queue
      q.publish "b" * 150_000
      msg = q.get(no_ack: true)
      msg.should_not be_nil
      msg.body_io.to_s.should eq "b" * 150_000 if msg
      conn.close
    end
  end
end
