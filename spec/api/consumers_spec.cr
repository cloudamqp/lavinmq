require "../spec_helper"

describe AvalancheMQ::ConsumersController do
  describe "GET /api/consumers" do
    it "should return all consumers" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("")
        q.subscribe {}
        response = get("http://localhost:8080/api/consumers")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["prefetch_count", "ack_required", "exclusive", "consumer_tag", "channel_details",
                "queue"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    ensure
      close(h, s)
    end

    it "should return empty array if no consumers" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      response = get("http://localhost:8080/api/consumers")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_true
    ensure
      close(h, s)
    end
  end

  describe "GET /api/consumers/vhost" do
    it "should return all consumers for a vhost" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("")
        q.subscribe {}
        response = get("http://localhost:8080/api/consumers/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 1
      end
    ensure
      close(h, s)
    end

    it "should return empty array if no consumers" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      response = get("http://localhost:8080/api/consumers/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_true
    ensure
      close(h, s)
    end
  end
end
