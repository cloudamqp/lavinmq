require "../spec_helper"
require "uri"

describe "rate-history log visibility" do
  describe "GET /api/channels" do
    it "omits the rate log, keeps the current rate" do
      with_http_server do |http, s|
        with_channel(s) do
          ms = JSON.parse(http.get("/api/channels").body).as_a.first["message_stats"]
          ms["publish_details"]["rate"]?.should_not be_nil
          ms["publish_details"]["log"]?.should be_nil
        end
      end
    end
  end

  describe "GET /api/channels/:name" do
    it "includes the rate log (detail page renders the chart)" do
      with_http_server do |http, s|
        with_channel(s) do
          name = URI.encode_path(JSON.parse(http.get("/api/channels").body).as_a.first["name"].as_s)
          ms = JSON.parse(http.get("/api/channels/#{name}").body)["message_stats"]
          ms["publish_details"]["log"]?.should_not be_nil
        end
      end
    end
  end

  describe "GET /api/connections" do
    it "omits the rate log, keeps the current rate" do
      with_http_server do |http, s|
        with_channel(s) do
          c = JSON.parse(http.get("/api/connections").body).as_a.last
          c["recv_oct_details"]["rate"]?.should_not be_nil
          c["recv_oct_details"]["log"]?.should be_nil
        end
      end
    end
  end

  describe "GET /api/connections/:name" do
    it "includes the rate log" do
      with_http_server do |http, s|
        with_channel(s) do
          name = URI.encode_path(JSON.parse(http.get("/api/connections").body).as_a.last["name"].as_s)
          c = JSON.parse(http.get("/api/connections/#{name}").body)
          c["recv_oct_details"]["log"]?.should_not be_nil
        end
      end
    end
  end
end
