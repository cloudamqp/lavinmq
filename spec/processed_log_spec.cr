require "./spec_helper"

private def ack_record(ack_ts : Int64, latency : Int64, consumer_tag : String = "ct",
                       headers : AMQ::Protocol::Table? = nil,
                       outcome : LavinMQ::ProcessedLog::Outcome = LavinMQ::ProcessedLog::Outcome::Ack,
                       redelivery_count : UInt32 = 0_u32,
                       payload_size : UInt32 = 42_u32)
  LavinMQ::ProcessedLog::Record.new(
    ack_ts_ms: ack_ts,
    latency_ms: latency,
    payload_size: payload_size,
    redelivery_count: redelivery_count,
    outcome: outcome,
    exchange: "ex",
    routing_key: "rk",
    consumer_tag: consumer_tag,
    headers: headers,
  )
end

describe LavinMQ::ProcessedLog do
  describe "unit" do
    it "records and queries entries" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 16)
        begin
          5.times do |i|
            log.record(ack_record(1_000_i64 + i, 10_i64 + i, "tag-#{i}"))
          end
          wait_for { log.query(0_i64, 10_000_i64, 0, 100).size == 5 }
          rows = log.query(0_i64, 10_000_i64, 0, 100)
          rows.size.should eq 5
          rows.first.ack_ts_ms.should eq 1_004
          rows.last.ack_ts_ms.should eq 1_000
          rows.map(&.consumer_tag).should contain "tag-0"
          rows.map(&.consumer_tag).should contain "tag-4"
        ensure
          log.close
        end
      end
    end

    it "computes summary percentiles, histogram and outcome counts" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 256)
        begin
          100.times do |i|
            outcome = i < 80 ? LavinMQ::ProcessedLog::Outcome::Ack : LavinMQ::ProcessedLog::Outcome::Reject
            log.record(ack_record(1_000_i64 + i, i.to_i64,
              outcome: outcome, redelivery_count: (i % 12).to_u32, payload_size: 100_u32))
          end
          wait_for { log.summary(0_i64, 10_000_i64).count == 100 }
          s = log.summary(0_i64, 10_000_i64)
          s.count.should eq 100
          s.outcomes["ack"].should eq 80
          s.outcomes["reject"].should eq 20
          s.latency_p50.should be_close(49, 5)
          s.latency_p95.should be_close(94, 5)
          s.latency_p99.should be_close(98, 5)
          s.redeliveries_max.should eq 11
          s.payload_size_avg.should eq 100
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
          1000.times do |i|
            log.record(ack_record(1_000_i64 + i, 1_i64, "", payload_size: 1_u32))
          end
          sleep 0.1.seconds
          summary = log.summary(0_i64, 10_000_i64)
          (summary.count + summary.dropped).should eq 1000
          summary.dropped.should be > 0
        ensure
          log.close
        end
      end
    end

    it "round-trips headers" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 16)
        begin
          table = AMQ::Protocol::Table.new
          table["x-tenant"] = "acme"
          table["x-region"] = "eu"
          table["count"] = 42_i64
          log.record(ack_record(2_000_i64, 5_i64, headers: table))
          wait_for { log.query(0_i64, 10_000_i64, 0, 10).size == 1 }
          rows = log.query(0_i64, 10_000_i64, 0, 10)
          rows.size.should eq 1
          h = rows.first.headers.not_nil!
          h["x-tenant"]?.should eq "acme"
          h["x-region"]?.should eq "eu"
          h["count"]?.should eq 42_i64
        ensure
          log.close
        end
      end
    end

    it "filters by outcome" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 64)
        begin
          log.record(ack_record(1_001_i64, 1_i64, "a"))
          log.record(ack_record(1_002_i64, 1_i64, "b", outcome: LavinMQ::ProcessedLog::Outcome::Reject))
          log.record(ack_record(1_003_i64, 1_i64, "c", outcome: LavinMQ::ProcessedLog::Outcome::Expired))
          wait_for { log.query(0_i64, 10_000_i64, 0, 100).size == 3 }
          acks = log.query(0_i64, 10_000_i64, 0, 100, LavinMQ::ProcessedLog::Outcome::Ack)
          acks.size.should eq 1
          acks.first.consumer_tag.should eq "a"
          rejects = log.query(0_i64, 10_000_i64, 0, 100, LavinMQ::ProcessedLog::Outcome::Reject)
          rejects.size.should eq 1
          rejects.first.consumer_tag.should eq "b"
        ensure
          log.close
        end
      end
    end

    it "filters by dotted-path header equality on nested tables" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 16)
        begin
          inner = AMQ::Protocol::Table.new
          inner["name"] = "api"
          inner["request_id"] = "abc-123"
          outer = AMQ::Protocol::Table.new
          outer["sender"] = inner
          other = AMQ::Protocol::Table.new
          other_inner = AMQ::Protocol::Table.new
          other_inner["name"] = "worker"
          other["sender"] = other_inner
          log.record(ack_record(1_001_i64, 1_i64, "a", headers: outer))
          log.record(ack_record(1_002_i64, 1_i64, "b", headers: other))
          wait_for { log.query(0_i64, 10_000_i64, 0, 100).size == 2 }
          matches = log.query(0_i64, 10_000_i64, 0, 100, nil, {"sender.name" => "api"})
          matches.size.should eq 1
          matches.first.consumer_tag.should eq "a"
        ensure
          log.close
        end
      end
    end

    it "filters by header equality" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 16)
        begin
          a = AMQ::Protocol::Table.new
          a["x-tenant"] = "acme"
          b = AMQ::Protocol::Table.new
          b["x-tenant"] = "other"
          log.record(ack_record(1_001_i64, 1_i64, "a", headers: a))
          log.record(ack_record(1_002_i64, 1_i64, "b", headers: b))
          log.record(ack_record(1_003_i64, 1_i64, "c", headers: nil))
          wait_for { log.query(0_i64, 10_000_i64, 0, 100).size == 3 }
          matches = log.query(0_i64, 10_000_i64, 0, 100, nil, {"x-tenant" => "acme"})
          matches.size.should eq 1
          matches.first.consumer_tag.should eq "a"
        ensure
          log.close
        end
      end
    end

    it "drops segments whose newest record is past retention on reopen" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 1_i64,
          segment_size: 200_i64, buffer_capacity: 256)
        begin
          20.times do |i|
            log.record(ack_record((RoughTime.unix_ms - 60_000) + i, 1_i64,
              "old-#{i}", payload_size: 50_u32))
          end
          wait_for { Dir.glob(File.join(dir, "processed.*")).size >= 2 }
          log.record(ack_record(RoughTime.unix_ms, 1_i64, "fresh", payload_size: 1_u32))
          log.close
          log2 = LavinMQ::ProcessedLog.new(dir, retention_ms: 1_i64,
            segment_size: 200_i64, buffer_capacity: 16)
          begin
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

    it "persists records across close/reopen including outcome and headers" do
      with_datadir do |dir|
        log = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 16)
        h = AMQ::Protocol::Table.new
        h["x-job"] = "build-42"
        log.record(ack_record(5_000_i64, 2_i64, "z",
          outcome: LavinMQ::ProcessedLog::Outcome::Reject, headers: h))
        wait_for { log.query(0_i64, 10_000_i64, 0, 10).size == 1 }
        log.close
        log2 = LavinMQ::ProcessedLog.new(dir, retention_ms: 86_400_000_i64,
          segment_size: 4_i64 * 1024 * 1024, buffer_capacity: 16)
        begin
          rows = log2.query(0_i64, 10_000_i64, 0, 10)
          rows.size.should eq 1
          rows.first.outcome.should eq LavinMQ::ProcessedLog::Outcome::Reject
          rows.first.headers.not_nil!["x-job"]?.should eq "build-42"
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
          rows.first.outcome.should eq LavinMQ::ProcessedLog::Outcome::Ack
          rows.first.routing_key.should eq "plog_q1"
          rows.first.redelivery_count.should eq 0
          rows.first.payload_size.should eq 2_u32
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
          rows.first.outcome.should eq LavinMQ::ProcessedLog::Outcome::Ack
          rows.first.redelivery_count.should eq 1
        end
      end
    end

    it "records a Reject row when reject(requeue=false) is sent" do
      with_http_server do |_http, s|
        with_channel(s) do |ch|
          q = ch.queue("plog_q4")
          server_q = s.vhosts["/"].queue("plog_q4").as(LavinMQ::AMQP::Queue)
          q.publish "drop"
          wait_for { server_q.message_count == 1 }
          q.subscribe(no_ack: false) do |msg|
            msg.reject(requeue: false)
          end
          wait_for { server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10).size == 1 }
          rows = server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10)
          rows.size.should eq 1
          rows.first.outcome.should eq LavinMQ::ProcessedLog::Outcome::Reject
          rows.first.consumer_tag.should_not eq ""
        end
      end
    end

    it "records an Expired row when message TTL elapses before consumption" do
      with_http_server do |_http, s|
        with_channel(s) do |ch|
          q = ch.queue("plog_q_expire")
          server_q = s.vhosts["/"].queue("plog_q_expire").as(LavinMQ::AMQP::Queue)
          props = AMQP::Client::Properties.new(expiration: "1") # 1 ms TTL
          q.publish("expire-me", props: props)
          wait_for(timeout: 3.seconds) {
            server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10).any? { |r| r.outcome == LavinMQ::ProcessedLog::Outcome::Expired }
          }
          rows = server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10)
          expired = rows.find { |r| r.outcome == LavinMQ::ProcessedLog::Outcome::Expired }
          expired.should_not be_nil
        end
      end
    end

    it "captures message headers on the recorded row" do
      with_http_server do |_http, s|
        with_channel(s) do |ch|
          q = ch.queue("plog_q_headers")
          server_q = s.vhosts["/"].queue("plog_q_headers").as(LavinMQ::AMQP::Queue)
          headers = AMQP::Client::Arguments.new({"x-tenant" => "acme", "x-region" => "eu"})
          q.publish("hello", props: AMQP::Client::Properties.new(headers: headers))
          wait_for { server_q.message_count == 1 }
          q.subscribe(no_ack: false) do |msg|
            msg.ack
          end
          wait_for { server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10).size == 1 }
          rows = server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10)
          h = rows.first.headers.not_nil!
          h["x-tenant"]?.should eq "acme"
          h["x-region"]?.should eq "eu"
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

    it "returns outcome breakdown via the summary endpoint" do
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
          body["outcomes"]["ack"].as_i.should eq 1
          body["redeliveries"]["histogram"].as_a.size.should eq 4
        end
      end
    end

    it "filters rows by header.* query params" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("plog_q_filter")
          server_q = s.vhosts["/"].queue("plog_q_filter").as(LavinMQ::AMQP::Queue)
          h1 = AMQP::Client::Arguments.new({"x-tenant" => "acme"})
          h2 = AMQP::Client::Arguments.new({"x-tenant" => "other"})
          q.publish("a", props: AMQP::Client::Properties.new(headers: h1))
          q.publish("b", props: AMQP::Client::Properties.new(headers: h2))
          wait_for { server_q.message_count == 2 }
          q.subscribe(no_ack: false, &.ack)
          wait_for { server_q.processed_query(0_i64, RoughTime.unix_ms + 1_000, 0, 10).size == 2 }
          to_ts = RoughTime.unix_ms + 1_000
          response = http.get("/api/queues/%2f/plog_q_filter/processed?from=0&to=#{to_ts}&header.x-tenant=acme")
          response.status_code.should eq 200
          body = JSON.parse(response.body).as_a
          body.size.should eq 1
          body[0]["headers"]["x-tenant"].as_s.should eq "acme"
        end
      end
    end
  end
end
