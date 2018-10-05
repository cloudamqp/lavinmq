require "../spec_helper"

describe AvalancheMQ::HTTPServer do
  describe "GET /api/overview" do
    it "should refuse access if no basic auth header" do
      response = HTTP::Client.get("#{BASE_URL}/api/overview")
      response.status_code.should eq 401
    end

    it "should refuse access if user does not exist" do
      s.users.delete("arnold")
      # arnold:pw
      response = get("/api/overview",
        headers: HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"})
      response.status_code.should eq 401
    end

    it "should refuse access if password does not match" do
      # guest:pw
      response = get("/api/overview",
        headers: HTTP::Headers{"Authorization" => "Basic Z3Vlc3Q6cHc="})
      response.status_code.should eq 401
    end

    it "should allow access if user is correct" do
      response = get("/api/overview")
      response.status_code.should eq 200
    end

    it "should filter stats if x-vhost header is set" do
      response = get("/api/whoami")
      response.status_code.should eq 200
    end
  end

  describe "GET /api/aliveness-test/vhost" do
    it "should run aliveness-test" do
      response = get("/api/aliveness-test/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["status"].as_s.should eq "ok"
    end
  end
end
