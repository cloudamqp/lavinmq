require "./spec_helper"
require "digest/sha256"
require "base64"

HEADERS = {"Cookie": "m=|:Z3Vlc3Q6Z3Vlc3Q%3D"}

private def inline_script_hashes(body : String) : Array(String)
  hashes = [] of String
  body.scan(/<script>(.*?)<\/script>/m) do |m|
    digest = Digest::SHA256.digest(m[1])
    hashes << "sha256-#{Base64.strict_encode(digest)}"
  end
  hashes
end

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

  it "GET / CSP header allows every inline script in the response body" do
    with_http_server do |http, _|
      response = http.get "/", HEADERS
      csp = response.headers["Content-Security-Policy"]
      hashes = inline_script_hashes(response.body)
      hashes.should_not be_empty
      hashes.each { |h| csp.should contain(h) }
    end
  end

  it "GET /login CSP header allows every inline script in the response body" do
    with_http_server do |http, _|
      response = http.get "/login"
      csp = response.headers["Content-Security-Policy"]
      hashes = inline_script_hashes(response.body)
      hashes.should_not be_empty
      hashes.each { |h| csp.should contain(h) }
    end
  end

  it "sets security headers on .html pages requested directly" do
    with_http_server do |http, _|
      response = http.get "/overview.html", HEADERS
      response.status_code.should eq 200
      response.headers["X-Frame-Options"].should eq "SAMEORIGIN"
      response.headers["Referrer-Policy"].should eq "same-origin"
      response.headers["Content-Security-Policy"]?.should_not be_nil
    end
  end

  it "redirects unauthenticated .html requests to login" do
    with_http_server do |http, _|
      response = http.get "/queues.html"
      response.status_code.should eq 307
      response.headers["Location"].should eq "login"
    end
  end

  it "serves login.html without requiring authentication" do
    with_http_server do |http, _|
      response = http.get "/login.html"
      response.status_code.should eq 200
      response.headers["Content-Security-Policy"]?.should_not be_nil
    end
  end
end
