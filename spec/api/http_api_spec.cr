require "../spec_helper"

describe AvalancheMQ::HTTPServer do
  it "should refuse access if no basic auth header" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    h = AvalancheMQ::HTTPServer.new(s, 8080)
    spawn { h.try &.listen }
    Fiber.yield
    response = HTTP::Client.get("http://localhost:8080/api/overview")
    response.status_code.should eq 401
  ensure
    h.try &.close
  end

  it "should refuse access if user does not exist" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    h = AvalancheMQ::HTTPServer.new(s, 8080)
    spawn { h.try &.listen }
    Fiber.yield
    # arnold:pw
    response = HTTP::Client.get("http://localhost:8080/api/overview",
                                headers: HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"})
    response.status_code.should eq 401
  ensure
    h.try &.close
  end

  it "should refuse access if password does not match" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    h = AvalancheMQ::HTTPServer.new(s, 8080)
    spawn { h.try &.listen }
    Fiber.yield
    # guest:pw
    response = HTTP::Client.get("http://localhost:8080/api/overview",
                                headers: HTTP::Headers{"Authorization" => "Basic Z3Vlc3Q6cHc="})
    response.status_code.should eq 401
  ensure
    h.try &.close
  end

  it "should allow access if user is correct" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    h = AvalancheMQ::HTTPServer.new(s, 8080)
    spawn { h.try &.listen }
    Fiber.yield
    response = HTTP::Client.get("http://localhost:8080/api/overview", headers: test_headers)
    response.status_code.should eq 200
  ensure
    h.try &.close
  end
end
