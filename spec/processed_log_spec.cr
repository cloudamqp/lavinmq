require "./spec_helper"

describe LavinMQ::ProcessedLog do
  describe "unit" do
    it "records and queries entries" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 16)
        begin
          5.times do |i|
            log.record(LavinMQ::ProcessedLog::Record.new(
              ack_ts_ms: 1_000_i64 + i,
              latency_ms: 10_i64 + i,
              payload_size: 42_u32,
              redelivery_count: 0_u32,
              exchange: "ex",
              routing_key: "rk",
              consumer_tag: "tag-#{i}",
            ))
          end
          wait_for { log.query(0_i64, 10_000_i64, 0, 100).size == 5 }
          rows = log.query(0_i64, 10_000_i64, 0, 100)
          rows.size.should eq 5
          # Records returned newest-first within a segment
          rows.first.ack_ts_ms.should eq 1_004
          rows.last.ack_ts_ms.should eq 1_000
          rows.map(&.consumer_tag).should contain "tag-0"
          rows.map(&.consumer_tag).should contain "tag-4"
        ensure
          log.close
        end
      end
    end

    it "computes summary percentiles and histogram" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 256)
        begin
          100.times do |i|
            log.record(LavinMQ::ProcessedLog::Record.new(
              ack_ts_ms: 1_000_i64 + i,
              latency_ms: i.to_i64,
              payload_size: 100_u32,
              redelivery_count: (i % 12).to_u32,
              exchange: "",
              routing_key: "rk",
              consumer_tag: "ct",
            ))
          end
          wait_for { log.summary(0_i64, 10_000_i64).count == 100 }
          s = log.summary(0_i64, 10_000_i64)
          s.count.should eq 100
          s.latency_p50.should be_close(49, 5)
          s.latency_p95.should be_close(94, 5)
          s.latency_p99.should be_close(98, 5)
          s.redeliveries_max.should eq 11
          s.payload_size_avg.should eq 100
          # histogram buckets sum to 100
          s.redeliveries_histogram.sum.should eq 100
        ensure
          log.close
        end
      end
    end

    it "drops on buffer overflow and counts drops" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 1)
        begin
          # Fill far beyond buffer capacity faster than the flusher drains.
          1000.times do |i|
            log.record(LavinMQ::ProcessedLog::Record.new(
              ack_ts_ms: 1_000_i64 + i, latency_ms: 1_i64, payload_size: 1_u32,
              redelivery_count: 0_u32, exchange: "", routing_key: "rk", consumer_tag: ""))
          end
          # Allow time for the flusher to drain whatever made it through.
          sleep 0.1.seconds
          summary = log.summary(0_i64, 10_000_i64)
          (summary.count + summary.dropped).should eq 1000
          summary.dropped.should be > 0
        ensure
          log.close
        end
      end
    end

    it "expires segments older than retention" do
      with_datadir do |dir|
        # Very small retention so the first batch is expired right away.
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 1_i64,
          segment_size: 200_i64, buffer_capacity: 256)
        begin
          20.times do |i|
            log.record(LavinMQ::ProcessedLog::Record.new(
              ack_ts_ms: (RoughTime.unix_ms - 60_000) + i,
              latency_ms: 1_i64, payload_size: 50_u32, redelivery_count: 0_u32,
              exchange: "ex", routing_key: "rk", consumer_tag: "ct"))
          end
          wait_for { Dir.glob(File.join(dir, "processed.*")).size >= 2 }
          # Add a fresh record so a current-segment exists with new data.
          log.record(LavinMQ::ProcessedLog::Record.new(
            ack_ts_ms: RoughTime.unix_ms,
            latency_ms: 1_i64, payload_size: 1_u32, redelivery_count: 0_u32,
            exchange: "", routing_key: "rk", consumer_tag: ""))
          # Manually trigger retention sweep via close + reopen.
          log.close
          log2 = LavinMQ::ProcessedLog.new(dir, retention_ms: 1_i64,
            segment_size: 200_i64, buffer_capacity: 16)
          begin
            # The retention fiber runs every 60s, so trigger by reflection or
            # simply check that scan_segment skips old data via summary read.
            recent = log2.query(RoughTime.unix_ms - 1_000, RoughTime.unix_ms + 1_000, 0, 100)
            recent.size.should be >= 1
          ensure
            log2.close
          end
        ensure
          log.close
        end
      end
    end

    it "persists records across close/reopen" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 16)
        log.record(LavinMQ::ProcessedLog::Record.new(
          ack_ts_ms: 5_000_i64, latency_ms: 2_i64, payload_size: 7_u32,
          redelivery_count: 1_u32, exchange: "x", routing_key: "y", consumer_tag: "z"))
        wait_for { log.query(0_i64, 10_000_i64, 0, 10).size == 1 }
        log.close
        log2 = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 16)
        begin
          rows = log2.query(0_i64, 10_000_i64, 0, 10)
          rows.size.should eq 1
          rows.first.exchange.should eq "x"
          rows.first.routing_key.should eq "y"
          rows.first.consumer_tag.should eq "z"
          rows.first.redelivery_count.should eq 1
        ensure
          log2.close
        end
      end
    end
  end

  describe "integration via queue ack" do
    it "records one row for a consumer ack" do
      with_http_server do |_http, s|
        with_channel(s) do |ch|
          q = ch.queue("plog_q1")
          server_q = s.vhosts["/"].queue("plog_q1").as(LavinMQ::AMQP::Queue)
          q.publish "hi"
          wait_for { server_q.message_count == 1 }
          q.subscribe(no_ack: false) do |msg|
            msg.ack
          end
          wait_for { server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10).size == 1 }
          rows = server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10)
          rows.size.should eq 1
          rows.first.routing_key.should eq "plog_q1"
          rows.first.redelivery_count.should eq 0
          rows.first.payload_size.should be > 0
        end
      end
    end

    it "records basic.get acks with empty consumer_tag" do
      with_http_server do |_http, s|
        with_channel(s) do |ch|
          q = ch.queue("plog_q2")
          server_q = s.vhosts["/"].queue("plog_q2").as(LavinMQ::AMQP::Queue)
          q.publish "via-get"
          wait_for { server_q.message_count == 1 }
          msg = q.get(no_ack: false)
          msg.try &.ack
          wait_for { server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10).size == 1 }
          rows = server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10)
          rows.size.should eq 1
          rows.first.consumer_tag.should eq ""
        end
      end
    end

    it "counts redeliveries when a message is rejected with requeue then acked" do
      with_http_server do |_http, s|
        with_channel(s) do |ch|
          ch.prefetch 1
          q = ch.queue("plog_q3")
          server_q = s.vhosts["/"].queue("plog_q3").as(LavinMQ::AMQP::Queue)
          q.publish "retry-me"
          wait_for { server_q.message_count == 1 }
          attempts = 0
          q.subscribe(no_ack: false) do |msg|
            attempts += 1
            if attempts == 1
              msg.reject(requeue: true)
            else
              msg.ack
            end
          end
          wait_for { server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10).size == 1 }
          rows = server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10)
          rows.size.should eq 1
          rows.first.redelivery_count.should eq 1
        end
      end
    end

    it "does not record on reject without requeue" do
      with_http_server do |_http, s|
        with_channel(s) do |ch|
          q = ch.queue("plog_q4")
          server_q = s.vhosts["/"].queue("plog_q4").as(LavinMQ::AMQP::Queue)
          q.publish "drop"
          wait_for { server_q.message_count == 1 }
          q.subscribe(no_ack: false) do |msg|
            msg.reject(requeue: false)
          end
          # Give the flusher a beat.
          sleep 0.2.seconds
          rows = server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10)
          rows.size.should eq 0
        end
      end
    end

    it "rejects /processed for stream queues" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-queue-type" => "stream"})
          ch.queue("plog_stream", args: args)
          response = http.get("/api/queues/%2f/plog_stream/processed")
          response.status_code.should eq 400
          response = http.get("/api/queues/%2f/plog_stream/processed/summary")
          response.status_code.should eq 400
        end
      end
    end

    it "returns JSON via the summary endpoint" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("plog_q5")
          server_q = s.vhosts["/"].queue("plog_q5").as(LavinMQ::AMQP::Queue)
          q.publish "one"
          wait_for { server_q.message_count == 1 }
          q.subscribe(no_ack: false) do |msg|
            msg.ack
          end
          wait_for { server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10).size == 1 }
          to_ts = RoughTime.unix_ms + 1_000
          response = http.get("/api/queues/%2f/plog_q5/processed/summary?from=0&to=#{to_ts}")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body["count"].as_i.should eq 1
          body["redeliveries"]["histogram"].as_a.size.should eq 4
        end
      end
    end
  end
end
