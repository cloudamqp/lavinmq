require "../spec_helper"
require "../../src/lavinmq/fiber_profiler"

describe LavinMQ::HTTP::ObserverController do
  describe "GET /api/observer" do
    it "returns 200 with fiber profile output" do
      LavinMQ::FiberProfiler.duration = 50.milliseconds
      with_http_server do |http, _|
        response = http.get("/api/observer")
        response.status_code.should eq 200
        response.headers["Content-Type"].should start_with("text/plain")
        response.body.should contain("context switches")
        response.body.should contain("Top")
        response.body.should contain("Resumes")
      end
    ensure
      LavinMQ::FiberProfiler.duration = 5.seconds
    end
  end
end
