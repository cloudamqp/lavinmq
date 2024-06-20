require "./spec_helper"

describe LavinMQ::HTTP::MainController do
  it "GET /" do
    with_http_server do |http, _|
      response = ::HTTP::Client.get "http://#{http.addr}"
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("text/html")
      response.body.should contain("LavinMQ")
    end
  end

  it "GET / includes head partial" do
    with_http_server do |http, _|
      response = ::HTTP::Client.get "http://#{http.addr}"
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("text/html")
      response.body.should contain("<title>")
    end
  end

  it "GET / includes header partial" do
    with_http_server do |http, _|
      response = ::HTTP::Client.get "http://#{http.addr}"
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("text/html")
      response.body.should contain("<header>")
    end
  end

  it "GET / includes footer partial" do
    with_http_server do |http, _|
      response = ::HTTP::Client.get "http://#{http.addr}"
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("text/html")
      response.body.should contain("<footer>")
    end
  end
end
