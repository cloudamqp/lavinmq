require "./spec_helper"

HEADERS = {"Cookie": "m=|:Z3Vlc3Q6Z3Vlc3Q%3D"}

describe LavinMQ::HTTP::MainController do
  it "GET /" do
    with_http_server do |http, _|
      response = http.get "/", HEADERS
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("text/html")
      response.body.should contain("LavinMQ")
    end
  end

  it "GET / includes head partial" do
    with_http_server do |http, _|
      response = http.get "/", HEADERS
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("text/html")
      response.body.should contain("<title>")
    end
  end

  it "GET / includes header partial" do
    with_http_server do |http, _|
      response = http.get "/", HEADERS
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("text/html")
      response.body.should contain("<header>")
    end
  end

  it "GET / includes footer partial" do
    with_http_server do |http, _|
      response = http.get "/", HEADERS
      response.status_code.should eq 200
      response.headers["Content-Type"].should contain("text/html")
      response.body.should contain("<footer>")
    end
  end

  it "GET / includes ETag header" do
    with_http_server do |http, _|
      response = http.get "/", HEADERS
      response.status_code.should eq 200
      response.headers["ETag"]?.should_not be_nil
      response.headers["ETag"].should start_with("W/\"")
    end
  end

  it "GET / returns 304 when If-None-Match matches ETag" do
    with_http_server do |http, _|
      response = http.get "/", HEADERS
      response.status_code.should eq 200
      etag = response.headers["ETag"]

      headers_with_etag = HEADERS.merge({"If-None-Match": etag})
      response = http.get "/", headers_with_etag
      response.status_code.should eq 304
      response.body.should be_empty
    end
  end

  it "GET / returns 200 when If-None-Match doesn't match ETag" do
    with_http_server do |http, _|
      headers_with_wrong_etag = HEADERS.merge({"If-None-Match": "W/\"wrong-etag\""})
      response = http.get "/", headers_with_wrong_etag
      response.status_code.should eq 200
      response.body.should_not be_empty
      response.body.should contain("LavinMQ")
    end
  end

  it "GET / ETag includes user tags" do
    with_http_server do |http, _|
      response = http.get "/", HEADERS
      response.status_code.should eq 200
      etag = response.headers["ETag"]
      etag.should match(/W\/"[^;]+;\d+"/)
    end
  end

  it "GET / ETag changes when user tags change" do
    with_http_server do |http, s|
      response1 = http.get "/", HEADERS
      response1.status_code.should eq 200
      etag1 = response1.headers["ETag"]

      user = s.users["guest"]
      original_tags = user.tags
      user.tags = [LavinMQ::Tag::Administrator, LavinMQ::Tag::Monitoring, LavinMQ::Tag::PolicyMaker]

      response2 = http.get "/", HEADERS
      response2.status_code.should eq 200
      etag2 = response2.headers["ETag"]

      etag1.should_not eq etag2

      user.tags = original_tags
    end
  end
end
