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

  describe "log contents" do
    # The stats loop sleeps an hour after its first sweep; the spec drives the sweeps itself.
    it "serves a queue's publish rate history in order" do
      orig = LavinMQ::Config.instance.stats_interval
      LavinMQ::Config.instance.stats_interval = 3_600_000
      begin
        with_http_server do |http, s|
          LavinMQ::Config.instance.stats_interval = 1000
          with_channel(s) do |ch|
            cq = ch.queue("logq")
            q = s.vhosts["/"].queue("logq")
            [5, 10, 0].each do |n|
              n.times { cq.publish_confirm "m" }
              q.update_rates
            end
            details = JSON.parse(http.get("/api/queues/%2f/logq").body)
            details["message_stats"]["publish_details"]["log"].as_a.map(&.as_f).should eq [5.0, 10.0, 0.0]
          end
        end
      ensure
        LavinMQ::Config.instance.stats_interval = orig
      end
    end

    it "logs the messages of all queues in the overview, also of since deleted ones" do
      orig = LavinMQ::Config.instance.stats_interval
      LavinMQ::Config.instance.stats_interval = 3_600_000
      begin
        with_http_server do |http, s|
          LavinMQ::Config.instance.stats_interval = 1000
          with_channel(s) do |ch|
            old = ch.queue("old-q")
            [1, 2, 3].each do |n|
              old.publish_confirm "m" if n > 1
              s.update_stats_rates
            end
            young = ch.queue("young-q")
            2.times { young.publish_confirm "m" }
            s.update_stats_rates
            young.delete
            s.update_stats_rates
            overview = JSON.parse(http.get("/api/overview").body)
            # old-q: 0, 1, 2, 2, 2 messages; young-q: 2 until deleted
            overview["queue_totals"]["messages_ready_log"].as_a.map(&.as_i).last(5).should eq [0, 1, 2, 4, 2]
          end
        end
      ensure
        LavinMQ::Config.instance.stats_interval = orig
      end
    end
  end
end
