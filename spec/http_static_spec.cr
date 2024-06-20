require "./spec_helper"

describe LavinMQ::HTTP::StaticController do
  it "GET /robots.txt" do
    with_http_server do |http, _|
      response = ::HTTP::Client.get "http://#{http.addr}/robots.txt"
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("text/plain")
      response.body.should contain("Disallow")
    end
  end

  it "GET /img/favicon.png" do
    with_http_server do |http, _|
      response = ::HTTP::Client.get "http://#{http.addr}/img/favicon.png"
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("image/png")
    end
  end
end
