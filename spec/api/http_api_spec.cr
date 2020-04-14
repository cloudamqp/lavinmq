require "../spec_helper"

describe AvalancheMQ::HTTP::Server do
  describe "GET /api/overview" do
    it "should refuse access if no basic auth header" do
      response = ::HTTP::Client.get("#{BASE_URL}/api/overview")
      response.status_code.should eq 401
    end

    it "should refuse access if user does not exist" do
      s.users.delete("arnold")
      # arnold:pw
      response = get("/api/overview",
        headers: ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"})
      response.status_code.should eq 401
    end

    it "should refuse access if password does not match" do
      # guest:pw
      response = get("/api/overview",
        headers: ::HTTP::Headers{"Authorization" => "Basic Z3Vlc3Q6cHc="})
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

    it "should return sum of all published messages" do
      close_servers
      TestHelpers.setup

      with_channel do |ch|
        q1 = ch.queue("stats_q1")
        q2 = ch.queue("stats_q2")
        5.times do
          q1.publish_confirm "m"
          q2.publish_confirm "m"
        end
      end

      response = get("/api/overview")
      count = JSON.parse(response.body).dig("message_stats", "publish")
      count.should eq 10
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

  describe "Pagination" do
    it "should page results" do
      response = get("/api/vhosts?page=1&page_size=1")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["filtered_count", "items", "item_count", "page", "page_size", "total_count"]
      keys.each { |k| body.as_h.keys.should contain(k) }
    end
  end

  describe "Sorting" do
    it "should sort results" do
      s.vhosts.create("x-vhost")
      s.vhosts.create("a-vhost")
      response = get("/api/vhosts?page=1&sort=name")
      response.status_code.should eq 200
      items = JSON.parse(response.body).as_h["items"].as_a
      items.first["name"].should eq "/"
      items.last["name"].should eq "x-vhost"
    end

    it "should sort reverse results" do
      s.vhosts.create("a-vhost")
      s.vhosts.create("x-vhost")
      response = get("/api/vhosts?page=1&sort=name&sort_reverse=true")
      response.status_code.should eq 200
      items = JSON.parse(response.body).as_h["items"].as_a
      items.first["name"].should eq "x-vhost"
      items.last["name"].should eq "/"
    end
  end
end
