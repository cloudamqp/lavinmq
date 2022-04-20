require "../spec_helper"

describe AvalancheMQ::HTTP::ConsumersController do
  describe "GET /metrics" do
    it "should return metrics in prometheus style" do
      response = get("/metrics")
      response.status_code.should eq 200
      response.body.lines.any?(&.starts_with? "telemetry_scrape_duration_seconds").should be_true
    end

    it "should support specifying prefix" do
      prefix = "testing"
      response = get("/metrics?prefix=#{prefix}")
      lines = response.body.lines
      metric_lines = lines.reject(&.starts_with? "#")
      prefix_lines = lines.select(&.starts_with? prefix)
      telemetry_lines = lines.select(&.starts_with? "telemetry")
      prefix_lines.size.should eq metric_lines.size - 2
      telemetry_lines.size.should eq 2
    end

    it "should not support a prefix longer than 20" do
      prefix = "abcdefghijklmnopqrstuvwxyz"
      response = get("/metrics?prefix=#{prefix}")
      response.status_code.should eq 400
      response.body.should eq "{\"error\":\"bad_request\",\"reason\":\"prefix to long\"}"
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
  end
end
