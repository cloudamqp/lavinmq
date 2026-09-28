require "../spec_helper"

describe LavinMQ::HTTP::BindingsController do
  describe "GET /api/bindings" do
    it "should return all bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_queue("bindings_q1", false, false)
        s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
        response = http.get("/api/bindings")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments",
                "properties_key"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end
  end

  describe "GET /api/bindings/vhost" do
    it "should return all bindings for a vhost" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_queue("bindings_q1", false, false)
        s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
        response = http.get("/api/bindings/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end
  end

  describe "GET /api/bindings/vhost/e/exchange/q/queue" do
    it "should return bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_queue("bindings_q1", false, false)
        s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
        response = http.get("/api/bindings/%2f/e/be1/q/bindings_q1")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end

    it "should return 404 if exchange does not exist" do
      with_http_server do |http, _|
        response = http.get("/api/bindings/%2f/e/404/q/404")
        response.status_code.should eq 404
      end
    end
  end

  describe "POST /api/bindings/vhost/e/exchange/q/queue" do
    it "should create binding" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_queue("bindings_q1", false, false)
        body = <<-JSON
          {
            "routing_key": "rk",
            "arguments": {}
          }
          JSON
        response = http.post("/api/bindings/%2f/e/be1/q/bindings_q1", body: body)
        response.status_code.should eq 201
        response.headers["Location"].should eq "bindings_q1/rk"
        s.vhosts["/"].exchange("be1").bindings_details.first.routing_key.should eq "rk"
      end
    end

    it "should inform about required fields" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_queue("bindings_q1", false, false)

        response = http.post("/api/bindings/%2f/e/be1/q/bindings_q1", body: "")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Field .+ is required/)
      end
    end

    it "should return 404 if exchange does not exist" do
      with_http_server do |http, _|
        response = http.get("/api/bindings/%2f/e/404/q/404")
        response.status_code.should eq 404
      end
    end

    it "should return forbidden for the default exchange" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("bindings_q2", false, false)
        body = <<-JSON
          {
            "routing_key": "rk",
            "arguments": {}
          }
          JSON
        response = http.post("/api/bindings/%2f/e/amq.default/q/bindings_q2", body: body)
        response.status_code.should eq 403
      end
    end

    it "should return bad request for invalid routing key on consistent hash exchange" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("ch1", "x-consistent-hash", false, false)
        s.vhosts["/"].declare_queue("bindings_q1", false, false)
        body = <<-JSON
          {
            "routing_key": "",
            "arguments": {}
          }
          JSON
        response = http.post("/api/bindings/%2f/e/ch1/q/bindings_q1", body: body)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("number")
      end
    end
  end

  describe "GET /api/bindings/vhost/e/exchange/q/queue/props" do
    it "should return binding" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_queue("bindings_q1", false, false)
        s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
        response = http.get("/api/bindings/%2f/e/be1/q/bindings_q1")
        binding = JSON.parse(response.body)
        props = binding[0]["properties_key"].as_s
        response = http.get("/api/bindings/%2f/e/be1/q/bindings_q1/#{props}")
        response.status_code.should eq 200
      end
    end
  end

  describe "DELETE /api/bindings/vhost/e/exchange/q/queue/props" do
    it "should delete binding" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_queue("bindings_q1", false, false)
        s.vhosts["/"].bind_queue("bindings_q1", "be1", ".*")
        response = http.get("/api/bindings/%2f/e/be1/q/bindings_q1")
        binding = JSON.parse(response.body)
        props = binding[0]["properties_key"].as_s
        response = http.delete("/api/bindings/%2f/e/be1/q/bindings_q1/#{props}")
        response.status_code.should eq 204
        s.vhosts["/"].exchange("be1").bindings_details.empty?.should be_true
      end
    end
  end

  describe "GET /api/bindings/vhost/e/source/e/destination" do
    it "should return bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_exchange("be2", "topic", false, false)
        s.vhosts["/"].bind_exchange("be2", "be1", ".*")
        response = http.get("/api/bindings/%2f/e/be1/e/be2")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end
  end

  describe "POST /api/bindings/vhost/e/source/e/destination" do
    it "should create binding" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_exchange("be2", "topic", false, false)
        body = <<-JSON
          {
            "routing_key": "rk",
            "arguments": {}
          }
          JSON
        response = http.post("/api/bindings/%2f/e/be1/e/be2", body: body)
        response.status_code.should eq 201
      end
    end

    it "should return forbidden for the default exchange" do
      with_http_server do |http, _|
        body = <<-JSON
          {
            "routing_key": "rk",
            "arguments": {}
          }
          JSON
        response = http.post("/api/bindings/%2f/e/amq.default/e/amq.direct", body: body)
        response.status_code.should eq 403
      end
    end
  end

  describe "GET /api/bindings/vhost/e/source/e/destination/props" do
    it "should return binding" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_exchange("be2", "topic", false, false)
        s.vhosts["/"].bind_exchange("be2", "be1", ".*")
        response = http.get("/api/bindings/%2f/e/be1/e/be2")
        binding = JSON.parse(response.body)
        props = binding[0]["properties_key"].as_s
        response = http.get("/api/bindings/%2f/e/be1/e/be2/#{props}")
        response.status_code.should eq 200
      end
    end
  end

  describe "DELETE /api/bindings/vhost/e/source/e/destination/props" do
    it "should delete binding" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        s.vhosts["/"].declare_exchange("be2", "topic", false, false)
        s.vhosts["/"].bind_exchange("be2", "be1", ".*")
        response = http.get("/api/bindings/%2f/e/be1/e/be2")
        binding = JSON.parse(response.body)
        props = binding[0]["properties_key"].as_s
        response = http.delete("/api/bindings/%2f/e/be1/e/be2/#{props}")
        response.status_code.should eq 204
      end
    end
  end

  describe "the mqtt exchange" do
    # mqtt.default is not in vhost.exchanges, so every route reaching it has to
    # resolve it by name.
    it "lists subscriptions among the bindings" do
      with_http_server do |http, s|
        declare_mqtt_subscription(s, "mqtt.listed", "a/b", 1u8)

        subscriptions = mqtt_bindings(http.get("/api/bindings"), "mqtt.listed")
        subscriptions.size.should eq 1
        subscriptions.first["routing_key"].should eq "a/b"
        subscriptions.first["arguments"][LavinMQ::MQTT::QOS_HEADER].should eq 1
        mqtt_bindings(http.get("/api/bindings/%2f"), "mqtt.listed").size.should eq 1
      end
    end

    it "lists subscriptions as bindings with the exchange as source" do
      with_http_server do |http, s|
        declare_mqtt_subscription(s, "mqtt.source", "a/b", 1u8)

        response = http.get("/api/exchanges/%2f/mqtt.default/bindings/source")
        response.status_code.should eq 200
        subscriptions = JSON.parse(response.body).as_a
        subscriptions.map(&.["destination"]).should eq ["mqtt.source"]
      end
    end

    # A single-segment topic filter, since the route takes `:props` rather than
    # `*props` and a filter with a slash in it can't be addressed through it.
    it "returns a single subscription by its properties key" do
      with_http_server do |http, s|
        declare_mqtt_subscription(s, "mqtt.props", "a", 1u8)

        subscriptions = mqtt_bindings(http.get("/api/bindings"), "mqtt.props")
        props = subscriptions.first["properties_key"].as_s
        response = http.get("/api/bindings/%2f/e/mqtt.default/q/mqtt.props/#{props}")
        response.status_code.should eq 200
        JSON.parse(response.body)["routing_key"].should eq "a"
      end
    end
  end
end

private def declare_mqtt_subscription(s, name, topic_filter, qos)
  mqtt_args = LavinMQ::AMQP::Table.new({"x-queue-type" => "mqtt"})
  s.vhosts["/"].declare_queue(name, true, false, mqtt_args)
  s.vhosts["/"].bind_queue(name, LavinMQ::MQTT::EXCHANGE, topic_filter,
    LavinMQ::MQTT.qos_arguments(qos))
end

private def mqtt_bindings(response, destination)
  response.status_code.should eq 200
  JSON.parse(response.body).as_a.select { |b| b["destination"] == destination }
end
