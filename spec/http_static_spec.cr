require "./spec_helper"

describe LavinMQ::HTTP::StaticController do
  it "GET /robots.txt" do
    response = ::HTTP::Client.get "#{BASE_URL}/robots.txt"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/plain")
    response.body.should contain("Disallow")
  end

  it "GET /img/favicon.png" do
    response = ::HTTP::Client.get "#{BASE_URL}/img/favicon.png"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("image/png")
  end

  it "GET /test/ serves index.html in the directory" do
    response = ::HTTP::Client.get "#{BASE_URL}/test/"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.body.should eq("This is a test\n")
  end

  it "GET /test (without trailing /) serves index.html in the directory" do
    response = ::HTTP::Client.get "#{BASE_URL}/test"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.body.should eq("This is a test\n")
  end

  it "GET /test/index.html" do
    response = ::HTTP::Client.get "#{BASE_URL}/test/index.html"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.body.should eq("This is a test\n")
  end
end
