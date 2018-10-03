require "../spec_helper"

describe AvalancheMQ::BindingsController do
  describe "GET /api/bindings" do
    it "should return all bindings" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("bindings_q1", false, false)
      s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("http://localhost:8080/api/bindings")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments",
              "properties_key"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_queue("bindings_q1")
    end
  end

  describe "GET /api/bindings/vhost" do
    it "should return all bindings for a vhost" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("bindings_q1", false, false)
      s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("http://localhost:8080/api/bindings/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_queue("bindings_q1")
    end
  end

  describe "GET /api/bindings/vhost/e/exchange/q/queue" do
    it "should return bindings" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("bindings_q1", false, false)
      s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("http://localhost:8080/api/bindings/%2f/e/be1/q/bindings_q1")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_queue("bindings_q1")
    end

    it "should return 404 if exchange does not exist" do
      response = get("http://localhost:8080/api/bindings/%2f/e/404/q/404")
      response.status_code.should eq 404
    end
  end

  describe "POST /api/bindings/vhost/e/exchange/q/queue" do
    it "should create binding" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("bindings_q1", false, false)
      body = %({
        "routing_key": "rk",
        "arguments": {}
      })
      response = post("http://localhost:8080/api/bindings/%2f/e/be1/q/bindings_q1", body: body)
      response.status_code.should eq 201
      response.headers["Location"].should match /api\/bindings\/%2f\/e\/be1\/q\/bindings_q1\/.*/
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_queue("bindings_q1")
    end

    it "should return 404 if exchange does not exist" do
      response = get("http://localhost:8080/api/bindings/%2f/e/404/q/404")
      response.status_code.should eq 404
    end
  end

  describe "GET /api/bindings/vhost/e/exchange/q/queue/props" do
    it "should return binding" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("bindings_q1", false, false)
      s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("http://localhost:8080/api/bindings/%2f/e/be1/q/bindings_q1")
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = get("http://localhost:8080/api/bindings/%2f/e/be1/q/bindings_q1/#{props}")
      response.status_code.should eq 200
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_queue("bindings_q1")
    end
  end

  describe "DELETE /api/bindings/vhost/e/exchange/q/queue/props" do
    it "should delete binding" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_queue("bindings_q1", false, false)
      s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("http://localhost:8080/api/bindings/%2f/e/be1/q/bindings_q1")
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = delete("http://localhost:8080/api/bindings/%2f/e/be1/q/bindings_q1/#{props}")
      response.status_code.should eq 204
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_queue("bindings_q1")
    end
  end

  describe "GET /api/bindings/vhost/e/source/e/destination" do
    it "should return bindings" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_exchange("be2", "topic", false, false)
      s.vhosts["/"].bind_exchange("be2", "be1", ".*")
      response = get("http://localhost:8080/api/bindings/%2f/e/be1/e/be2")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_exchange("be2")
      s.vhosts["/"].delete_queue("bindings_q1")
    end
  end

  describe "POST /api/bindings/vhost/e/source/e/destination" do
    it "should create binding" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_exchange("be2", "topic", false, false)
      body = %({
        "routing_key": "rk",
        "arguments": {}
      })
      response = post("http://localhost:8080/api/bindings/%2f/e/be1/e/be2", body: body)
      response.status_code.should eq 201
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_exchange("be2")
    end
  end

  describe "GET /api/bindings/vhost/e/source/e/destination/props" do
    it "should return binding" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_exchange("be2", "topic", false, false)
      s.vhosts["/"].bind_exchange("be2", "be1", ".*")
      response = get("http://localhost:8080/api/bindings/%2f/e/be1/e/be2")
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = get("http://localhost:8080/api/bindings/%2f/e/be1/e/be2/#{props}")
      response.status_code.should eq 200
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_exchange("be2")
    end
  end

  describe "DELETE /api/bindings/vhost/e/source/e/destination/props" do
    it "should delete binding" do
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      s.vhosts["/"].declare_exchange("be2", "topic", false, false)
      s.vhosts["/"].bind_exchange("be2", "be1", ".*")
      response = get("http://localhost:8080/api/bindings/%2f/e/be1/e/be2")
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = delete("http://localhost:8080/api/bindings/%2f/e/be1/e/be2/#{props}")
      response.status_code.should eq 204
    ensure
      s.vhosts["/"].delete_exchange("be1")
      s.vhosts["/"].delete_exchange("be2")
    end
  end
end
