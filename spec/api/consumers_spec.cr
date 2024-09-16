require "../spec_helper"

describe LavinMQ::HTTP::ConsumersController do
  describe "GET /api/consumers" do
    it "should return all consumers" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("")
          q.subscribe { }
          response = http.get("/api/consumers")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.empty?.should be_false
          keys = ["prefetch_count", "ack_required", "exclusive", "consumer_tag", "channel_details",
                  "queue"]
          body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
        end
      end
    end

    it "should return empty array if no consumers" do
      with_http_server do |http, _|
        response = http.get("/api/consumers")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_true
      end
    end
  end

  describe "GET /api/consumers/vhost" do
    it "should return all consumers for a vhost" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("")
          q.subscribe { }
          sleep 10.milliseconds
          response = http.get("/api/consumers/%2f")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.size.should eq 1
        end
      end
    end

    it "should return empty array if no consumers" do
      with_http_server do |http, _|
        response = http.get("/api/consumers/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_true
      end
    end
  end

  describe "DELETE /api/consumers/vhost/connection/channel/consumer" do
    it "should return 204 when successful" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("")
          consumer = q.subscribe { }
          sleep 10.milliseconds
          conn = s.connections.to_a.last.name
          response = http.delete("/api/consumers/%2f/#{URI.encode_path(conn)}/#{ch.id}/#{consumer}")
          response.status_code.should eq 204
          wait_for { ch.has_subscriber?(consumer) == false }
          ch.has_subscriber?(consumer).should be_false
        end
      end
    end

    it "should return 404 if connection does not exist" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("")
          consumer = q.subscribe { }
          sleep 10.milliseconds
          response = http.delete("/api/consumers/%2f/#{URI.encode_path("abc")}/#{ch.id}/#{consumer}")
          response.status_code.should eq 404
        end
      end
    end

    it "should return 404 if channel does not exist" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          conn = s.connections.first.name
          q = ch.queue("")
          consumer = q.subscribe { }
          sleep 10.milliseconds
          response = http.delete("/api/consumers/%2f/#{URI.encode_path(conn)}/123/#{consumer}")
          response.status_code.should eq 404
        end
      end
    end

    it "should return 404 if consumer does not exist" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          conn = s.connections.first.name
          q = ch.queue("")
          q.subscribe { }
          sleep 10.milliseconds
          response = http.delete("/api/consumers/%2f/#{URI.encode_path(conn)}/#{ch.id}/test")
          response.status_code.should eq 404
        end
      end
    end
  end
end
