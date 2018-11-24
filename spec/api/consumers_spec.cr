require "../spec_helper"

describe AvalancheMQ::HTTP::ConsumersController do
  describe "GET /api/consumers" do
    it "should return all consumers" do
      with_channel do |ch|
        q = ch.queue("")
        q.subscribe { }
        response = get("/api/consumers")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["prefetch_count", "ack_required", "exclusive", "consumer_tag", "channel_details",
                "queue"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "should return empty array if no consumers" do
      response = get("/api/consumers")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_true
    end
  end

  describe "GET /api/consumers/vhost" do
    it "should return all consumers for a vhost" do
      with_channel do |ch|
        q = ch.queue("")
        q.subscribe { }
        sleep 0.1
        response = get("/api/consumers/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 1
      end
    end

    it "should return empty array if no consumers" do
      response = get("/api/consumers/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_true
    end
  end
end
