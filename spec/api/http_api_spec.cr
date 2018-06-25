require "../spec_helper"

describe AvalancheMQ::HTTPServer do
  describe "GET /api/overview" do
    it "should refuse access if no basic auth header" do
      s, h = create_servers
      listen(h)
      response = HTTP::Client.get("http://localhost:8080/api/overview")
      response.status_code.should eq 401
    ensure
      close(h)
    end

    it "should refuse access if user does not exist" do
      s, h = create_servers
      listen(h)
      s.users.delete("arnold")
      # arnold:pw
      response = get("http://localhost:8080/api/overview",
        headers: HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"})
      response.status_code.should eq 401
    ensure
      close(h)
    end

    it "should refuse access if password does not match" do
      s, h = create_servers
      listen(h)
      # guest:pw
      response = get("http://localhost:8080/api/overview",
        headers: HTTP::Headers{"Authorization" => "Basic Z3Vlc3Q6cHc="})
      response.status_code.should eq 401
    ensure
      close(h)
    end

    it "should allow access if user is correct" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/overview")
      response.status_code.should eq 200
    ensure
      close(h)
    end

    it "should filter stats if x-vhost header is set" do
      s, h = create_servers
      listen(h)
      headers = HTTP::Headers{"x-vhost" => "/"}
      response = get("http://localhost:8080/api/overview", headers: headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["object_totals"]["exchanges"].should eq 6
    ensure
      close(h)
    end
  end

  describe "GET /api/whoami" do
    it "should return details of the currently authenticated user" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/whoami")
      response.status_code.should eq 200
    ensure
      close(h)
    end
  end

  describe "GET /api/aliveness-test/vhost" do
    it "should run aliveness-test" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/aliveness-test/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["status"].as_s.should eq "ok"
    ensure
      close(h)
    end
  end
end
