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

  # The stats loop sleeps an hour after its first sweep; the specs drive the sweeps themselves.
  describe "log contents" do
    it "serves a queue's publish rate history in order" do
      with_stats_interval(3_600_000) do
        with_http_server do |http, s|
          with_channel(s) do |ch|
            cq = ch.queue("logq")
            q = s.vhosts["/"].queue("logq")
            [5, 5, 10, 0].each do |n|
              n.times { cq.publish_confirm "m" }
              q.update_rates(1.0)
            end
            details = JSON.parse(http.get("/api/queues/%2f/logq").body)
            # The first tick spans a partial interval and is no sample
            details["message_stats"]["publish_details"]["log"].as_a.map(&.as_f).should eq [5.0, 10.0, 0.0]
          end
        end
      end
    end

    it "logs the messages of all queues in the overview, also of since deleted ones" do
      with_stats_interval(3_600_000) do
        with_http_server do |http, s|
          with_channel(s) do |ch|
            old = ch.queue("old-q")
            s.update_stats_rates(1.0)
            2.times { old.publish_confirm "m"; s.update_stats_rates(1.0) }
            young = ch.queue("young-q")
            2.times { young.publish_confirm "m" }
            s.update_stats_rates(1.0)
            young.delete
            s.update_stats_rates(1.0)
            overview = JSON.parse(http.get("/api/overview").body)
            # old-q: 0, 1, 2, 2, 2 messages; young-q: 2 until deleted
            overview["queue_totals"]["messages_ready_log"].as_a.map(&.as_i).last(5).should eq [0, 1, 2, 4, 2]
          end
        end
      end
    end
  end
end
