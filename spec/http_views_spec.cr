require "./spec_helper"

describe LavinMQ::HTTP::MainController do
  it "GET /" do
    response = ::HTTP::Client.get BASE_URL
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.body.should contain("LavinMQ")
  end

  it "GET / includes head partial" do
    response = ::HTTP::Client.get BASE_URL
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.body.should contain("<title>")
  end

  it "GET / includes header partial" do
    response = ::HTTP::Client.get BASE_URL
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.body.should contain("<header>")
  end

  it "GET / includes footer partial" do
    response = ::HTTP::Client.get BASE_URL
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.body.should contain("<footer>")
  end
end
