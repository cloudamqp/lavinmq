require "../spec_helper"

describe LavinMQ::HTTP::BindingsController do
  describe "GET /api/bindings" do
    it "should return all bindings" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_queue("bindings_q1", false, false)
      Server.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("/api/bindings")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments",
              "properties_key"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end
  end

  describe "GET /api/bindings/vhost" do
    it "should return all bindings for a vhost" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_queue("bindings_q1", false, false)
      Server.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("/api/bindings/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end
  end

  describe "GET /api/bindings/vhost/e/exchange/q/queue" do
    it "should return bindings" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_queue("bindings_q1", false, false)
      Server.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("/api/bindings/%2f/e/be1/q/bindings_q1")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end

    it "should return 404 if exchange does not exist" do
      response = get("/api/bindings/%2f/e/404/q/404")
      response.status_code.should eq 404
    end
  end

  describe "POST /api/bindings/vhost/e/exchange/q/queue" do
    it "should create binding" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_queue("bindings_q1", false, false)
      body = %({
        "routing_key": "rk",
        "arguments": {}
      })
      response = post("/api/bindings/%2f/e/be1/q/bindings_q1", body: body)
      response.status_code.should eq 201
      response.headers["Location"].should eq "bindings_q1/rk"
      Server.vhosts["/"].exchanges["be1"].queue_bindings.last_key.first.should eq "rk"
    end

    it "should inform about required fields" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_queue("bindings_q1", false, false)

      response = post("/api/bindings/%2f/e/be1/q/bindings_q1", body: "")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Field .+ is required/)
    end

    it "should return 404 if exchange does not exist" do
      response = get("/api/bindings/%2f/e/404/q/404")
      response.status_code.should eq 404
    end

    it "should return forbidden for the default exchange" do
      Server.vhosts["/"].declare_queue("bindings_q2", false, false)
      body = %({
        "routing_key": "rk",
        "arguments": {}
      })
      response = post("/api/bindings/%2f/e/amq.default/q/bindings_q2", body: body)
      response.status_code.should eq 403
    end
  end

  describe "GET /api/bindings/vhost/e/exchange/q/queue/props" do
    it "should return binding" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_queue("bindings_q1", false, false)
      Server.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("/api/bindings/%2f/e/be1/q/bindings_q1")
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = get("/api/bindings/%2f/e/be1/q/bindings_q1/#{props}")
      response.status_code.should eq 200
    end
  end

  describe "DELETE /api/bindings/vhost/e/exchange/q/queue/props" do
    it "should delete binding" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_queue("bindings_q1", false, false)
      Server.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
      response = get("/api/bindings/%2f/e/be1/q/bindings_q1")
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = delete("/api/bindings/%2f/e/be1/q/bindings_q1/#{props}")
      response.status_code.should eq 204
      Server.vhosts["/"].exchanges["be1"].queue_bindings.empty?.should be_true
    end
  end

  describe "GET /api/bindings/vhost/e/source/e/destination" do
    it "should return bindings" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_exchange("be2", "topic", false, false)
      Server.vhosts["/"].bind_exchange("be2", "be1", ".*")
      response = get("/api/bindings/%2f/e/be1/e/be2")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end
  end

  describe "POST /api/bindings/vhost/e/source/e/destination" do
    it "should create binding" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_exchange("be2", "topic", false, false)
      body = %({
        "routing_key": "rk",
        "arguments": {}
      })
      response = post("/api/bindings/%2f/e/be1/e/be2", body: body)
      response.status_code.should eq 201
    end

    it "should return forbidden for the default exchange" do
      body = %({
        "routing_key": "rk",
        "arguments": {}
      })
      response = post("/api/bindings/%2f/e/amq.default/e/amq.direct", body: body)
      response.status_code.should eq 403
    end
  end

  describe "GET /api/bindings/vhost/e/source/e/destination/props" do
    it "should return binding" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_exchange("be2", "topic", false, false)
      Server.vhosts["/"].bind_exchange("be2", "be1", ".*")
      response = get("/api/bindings/%2f/e/be1/e/be2")
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = get("/api/bindings/%2f/e/be1/e/be2/#{props}")
      response.status_code.should eq 200
    end
  end

  describe "DELETE /api/bindings/vhost/e/source/e/destination/props" do
    it "should delete binding" do
      Server.vhosts["/"].declare_exchange("be1", "topic", false, false)
      Server.vhosts["/"].declare_exchange("be2", "topic", false, false)
      Server.vhosts["/"].bind_exchange("be2", "be1", ".*")
      response = get("/api/bindings/%2f/e/be1/e/be2")
      binding = JSON.parse(response.body)
      props = binding[0]["properties_key"].as_s
      response = delete("/api/bindings/%2f/e/be1/e/be2/#{props}")
      response.status_code.should eq 204
    end
  end
end
