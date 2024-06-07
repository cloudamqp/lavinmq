require "../spec_helper"
require "string_scanner"

describe LavinMQ::HTTP::ConsumersController do
  describe "GET /metrics" do
    it "should return metrics in prometheus style" do
      response = get("/metrics")
      response.status_code.should eq 200
      response.body.lines.any?(&.starts_with? "telemetry_scrape_duration_seconds").should be_true
    end

    it "should perform sanity check on sampled metrics" do
      vhost = Server.vhosts.create("pmths")
      vhost.declare_queue("test1", true, false)
      vhost.delete_queue("test1")
      vhost.declare_queue("test2", true, false)
      raw = get("/metrics").body
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

    it "should support specifying prefix" do
      prefix = "testing"
      response = get("/metrics?prefix=#{prefix}")
      lines = response.body.lines
      lines.count(&.starts_with? "telemetry").should eq 2
      metric_lines = lines.reject(&.starts_with? "#")
      prefix_lines = lines.select(&.starts_with? prefix)
      prefix_lines.size.should eq metric_lines.size - 2
    end

    it "should not support a prefix longer than 20" do
      prefix = "abcdefghijklmnopqrstuvwxyz"
      response = get("/metrics?prefix=#{prefix}")
      response.status_code.should eq 400
      response.body.should match /Prefix too long/
    end
  end

  describe "GET /metrics/detailed" do
    it "should support specifying families" do
      response = get("/metrics/detailed?family=connection_churn_metrics")
      response.status_code.should eq 200
      lines = response.body.lines
      lines.any?(&.starts_with? "lavinmq_detailed_connections_opened_total").should be_true

      response = get("/metrics/detailed?family=family=queue_coarse_metrics")
      response.status_code.should eq 200
      lines = response.body.lines
      lines.any?(&.starts_with? "lavinmq_detailed_connections_opened_total").should be_false
    end

    it "should perform sanity check on sampled metrics" do
      Server.vhosts.create("pmths")
      conn1 = AMQP::Client.new(SpecHelper.amqp_base_url).connect
      conn2 = AMQP::Client.new(SpecHelper.amqp_base_url).connect
      conn1.close(no_wait: true)
      raw = get("/metrics/detailed?family=connection_churn_metrics").body
      parsed_metrics = PrometheusSpecHelper.parse_prometheus(raw)
      parsed_metrics.find! { |metric| metric[:key] == "lavinmq_detailed_connections_opened_total" }[:value].should eq 2
      parsed_metrics.find! { |metric| metric[:key] == "lavinmq_detailed_connections_closed_total" }[:value].should eq 1
      conn2.close(no_wait: true)
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
