require "../spec_helper"
require "string_scanner"

describe LavinMQ::HTTP::PrometheusController do
  describe "authentication" do
    it "should allow unauthenticated access to metrics on metrics port" do
      with_metrics_server do |http, _|
        response = http.get("/metrics")
        response.status_code.should eq 200
      end
    end
    it "should require authenticated access to metrics on mgmt port" do
      with_http_server do |http, _|
        response = HTTP::Client.get("http://#{http.addr}/metrics")
        response.status_code.should eq 401
      end
    end
  end
  describe "GET /metrics" do
    it "should return metrics in prometheus style" do
      with_metrics_server do |http, _|
        response = http.get("/metrics")
        response.status_code.should eq 200
        response.body.lines.any?(&.starts_with? "telemetry_scrape_duration_seconds").should be_true
      end
    end

    it "should perform sanity check on sampled metrics" do
      with_metrics_server do |http, s|
        vhost = s.vhosts.create("pmths")
        vhost.declare_queue("test1", true, false)
        vhost.delete_queue("test1")
        vhost.declare_queue("test2", true, false)
        raw = http.get("/metrics").body
        parsed_metrics = PrometheusSpecHelper.parse_prometheus(raw)
        parsed_metrics.each do |metric|
          case metric[:key]
          when "lavinmq_queues_declared_total"
            metric[:value].should eq 2
          when "lavinmq_queues"
            metric[:value].should eq 1
          end
        end
      end
    end

    it "should count all delivered messages (both get and deliver)" do
      with_metrics_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("test_deliver")
          ch.prefetch(1)

          # Get baseline
          raw = http.get("/metrics").body
          parsed_metrics = PrometheusSpecHelper.parse_prometheus(raw)
          delivered = parsed_metrics.find { |m| m[:key] == "lavinmq_global_messages_delivered_total" }
          baseline = delivered.try(&.[:value]) || 0

          # Publish 3 messages
          3.times { q.publish "test message" }

          # Deliver 1 message via get (polling consumer)
          msg = q.get(no_ack: false)
          msg.should_not be_nil
          msg.try &.ack

          # Deliver 2 messages via subscribe (push consumer)
          delivered_count = 0
          q.subscribe(no_ack: false) do |delivery|
            delivered_count += 1
            delivery.ack
          end
          wait_for { delivered_count == 2 }

          # Check metrics - should count all 3 deliveries
          raw = http.get("/metrics").body
          parsed_metrics = PrometheusSpecHelper.parse_prometheus(raw)
          delivered = parsed_metrics.find { |m| m[:key] == "lavinmq_global_messages_delivered_total" }
          delivered.should_not be_nil
          delivered.try(&.[:value].should eq(baseline + 3))
        end
      end
    end

    it "should support specifying prefix" do
      with_metrics_server do |http, _|
        prefix = "testing"
        response = http.get("/metrics?prefix=#{prefix}")
        lines = response.body.lines
        lines.count(&.starts_with? "telemetry").should eq 2
        metric_lines = lines.reject(&.starts_with? "#")
        prefix_lines = lines.select(&.starts_with? prefix)
        prefix_lines.size.should eq metric_lines.size - 2
      end
    end

    it "should not support a prefix longer than 20" do
      with_metrics_server do |http, _|
        prefix = "abcdefghijklmnopqrstuvwxyz"
        response = http.get("/metrics?prefix=#{prefix}")
        response.status_code.should eq 400
        response.body.should match /Prefix too long/
      end
    end

    it "should include mfile count metric" do
      with_metrics_server do |http, _|
        response = http.get("/metrics")
        response.status_code.should eq 200
        parsed_metrics = PrometheusSpecHelper.parse_prometheus(response.body)
        mfile_metric = parsed_metrics.find { |m| m[:key] == "lavinmq_mfile_count" }
        mfile_metric.should_not be_nil
        mfile_metric.not_nil![:value].should be >= 0
        {% if flag?(:linux) %}
          mmap_metric = parsed_metrics.find { |m| m[:key] == "lavinmq_process_mmap_count" }
          mmap_metric.should_not be_nil
          mmap_metric.not_nil![:value].should be > 0
          limit_metric = parsed_metrics.find { |m| m[:key] == "lavinmq_process_mmap_limit" }
          limit_metric.should_not be_nil
          limit_metric.not_nil![:value].should be > 0
        {% end %}
      end
    end
  end

  describe "vhost access control" do
    it "should only return metrics for vhosts the user has access to" do
      with_http_server do |http, s|
        # Create two vhosts
        vhost1 = s.vhosts.create("vhost1")
        vhost2 = s.vhosts.create("vhost2")

        # Create two users with management tag (required for metrics access)
        s.users.create("user1", "pass1", [LavinMQ::Tag::Management])
        s.users.create("user2", "pass2", [LavinMQ::Tag::Management])

        # Give each user access only to their respective vhost
        s.users.add_permission("user1", "vhost1", /.*/, /.*/, /.*/)
        s.users.add_permission("user2", "vhost2", /.*/, /.*/, /.*/)

        # Create a queue in each vhost
        vhost1.declare_queue("queue_in_vhost1", true, false)
        vhost2.declare_queue("queue_in_vhost2", true, false)

        # user1 should only see vhost1 queue metrics
        user1_auth = "Basic #{Base64.strict_encode("user1:pass1")}"
        response = http.get("/metrics/detailed?family=queue_coarse_metrics", HTTP::Headers{"Authorization" => user1_auth})
        response.status_code.should eq 200
        parsed = PrometheusSpecHelper.parse_prometheus(response.body)

        user1_queues = parsed.select { |m| m[:key] == "lavinmq_detailed_queue_messages_ready" }
        user1_queues.size.should eq 1
        user1_queues.first[:attrs]["queue"].should eq "queue_in_vhost1"
        user1_queues.first[:attrs]["vhost"].should eq "vhost1"

        # user2 should only see vhost2 queue metrics
        user2_auth = "Basic #{Base64.strict_encode("user2:pass2")}"
        response = http.get("/metrics/detailed?family=queue_coarse_metrics", HTTP::Headers{"Authorization" => user2_auth})
        response.status_code.should eq 200
        parsed = PrometheusSpecHelper.parse_prometheus(response.body)

        user2_queues = parsed.select { |m| m[:key] == "lavinmq_detailed_queue_messages_ready" }
        user2_queues.size.should eq 1
        user2_queues.first[:attrs]["queue"].should eq "queue_in_vhost2"
        user2_queues.first[:attrs]["vhost"].should eq "vhost2"
      end
    end
  end

  describe "GET /metrics/detailed" do
    it "should support specifying families" do
      with_metrics_server do |http, _|
        response = http.get("/metrics/detailed?family=connection_churn_metrics")
        response.status_code.should eq 200
        lines = response.body.lines
        lines.any?(&.starts_with? "lavinmq_detailed_connections_opened_total").should be_true

        response = http.get("/metrics/detailed?family=family=queue_coarse_metrics")
        response.status_code.should eq 200
        lines = response.body.lines
        lines.any?(&.starts_with? "lavinmq_detailed_connections_opened_total").should be_false
      end
    end

    it "should perform sanity check on sampled metrics" do
      with_metrics_server do |http, s|
        with_channel(s) do |_|
        end
        with_channel(s) do |_|
          raw = http.get("/metrics/detailed?family=connection_churn_metrics").body
          parsed_metrics = PrometheusSpecHelper.parse_prometheus(raw)
          parsed_metrics.find! { |metric| metric[:key] == "lavinmq_detailed_connections_opened_total" }[:value].should eq 2
          parsed_metrics.find! { |metric| metric[:key] == "lavinmq_detailed_connections_closed_total" }[:value].should eq 1
        end
      end
    end

    it "should group TYPE and HELP by metric name" do
      with_metrics_server do |http, s|
        vhost = s.vhosts.create("grouping_test")
        vhost.declare_queue("queue_1", true, false)
        vhost.declare_queue("queue_2", true, false)
        vhost.declare_queue("queue_3", true, false)

        raw = http.get("/metrics/detailed?family=queue_coarse_metrics").body
        lines = raw.lines

        # Count occurrences of TYPE declarations for detailed_queue_messages_ready
        type_count = lines.count(&.includes?("# TYPE lavinmq_detailed_queue_messages_ready"))
        help_count = lines.count(&.includes?("# HELP lavinmq_detailed_queue_messages_ready"))

        # Should appear exactly once, not once per queue
        type_count.should eq 1
        help_count.should eq 1

        # But values should appear for each queue (3 queues)
        value_count = lines.count(&.starts_with?("lavinmq_detailed_queue_messages_ready{"))
        value_count.should eq 3

        # Verify grouping: TYPE and HELP should immediately precede the values
        # Find TYPE line index, HELP should be next, then values should follow
        type_idx = lines.index(&.includes?("# TYPE lavinmq_detailed_queue_messages_ready"))
        help_idx = lines.index(&.includes?("# HELP lavinmq_detailed_queue_messages_ready"))
        first_value_idx = lines.index(&.starts_with?("lavinmq_detailed_queue_messages_ready{"))

        type_idx.should_not be_nil
        help_idx.should_not be_nil
        first_value_idx.should_not be_nil

        # HELP should come right after TYPE
        help_idx.should eq(type_idx.not_nil! + 1)
        # First value should come right after HELP
        first_value_idx.should eq(help_idx.not_nil! + 1)
      end
    end
  end
end

class PrometheusSpecHelper
  class Invalid < Exception
    def initialize
      super("invalid input")
    end
  end

  KEY_RE        = /[\w:]+/
  VALUE_RE      = /-?\d+\.?\d*E?-?\d*|NaN/
  ATTR_KEY_RE   = /[ \w-]+/
  ATTR_VALUE_RE = %r{\s*"([\\"'\sa-zA-Z0-9\-_/.+]*)"\s*}

  def self.parse_prometheus(raw)
    s = StringScanner.new(raw)
    res = [] of NamedTuple(key: String, attrs: Hash(String, String), value: Float64)
    until s.eos?
      if s.peek(1) == "#"
        s.scan(/.*\n/)
        next
      end
      key = s.scan KEY_RE
      raise Invalid.new unless key
      attrs = parse_attrs(s)
      value = s.scan VALUE_RE
      raise Invalid.new unless value
      value = value.to_f
      s.scan(/\n/)
      res.push({key: key, attrs: attrs, value: value})
    end
    res
  end

  private def self.parse_attrs(s)
    attrs = Hash(String, String).new
    if s.scan(/\s|{/) == "{"
      loop do
        if s.peek(1) == "}"
          s.scan(/}/)
          break
        end
        key = s.scan ATTR_KEY_RE
        raise Invalid.new unless key
        key = key.strip
        s.scan(/=/)
        s.scan ATTR_VALUE_RE

        value = s[1]
        raise Invalid.new unless value
        attrs[key] = value
        break if s.scan(/,|}/) == "}"
      end
      s.scan(/\s/)
    end
    attrs
  end
end
