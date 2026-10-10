require "./spec_helper"

describe LavinMQ::HTTP::Server do
  it "answers requests on a kept-alive connection without waiting for delayed ACKs" do
    with_http_server do |http, _s|
      client = HTTP::Client.new(http.@addr.address, http.@addr.port)
      client.basic_auth("guest", "guest")
      client.get("/api/overview").status_code.should eq 200
      times = (1..3).map { Time.measure { client.get("/api/overview") }.total_milliseconds }
      client.close
      times.sort[1].should be < 20 # ~40 ms when Nagle holds the response's last segment
    end
  end
end
