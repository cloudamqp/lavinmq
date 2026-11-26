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
    it "should refuse binding a delayed exchange to its internal queue" do
      with_http_server do |http, s|
        args = LavinMQ::AMQP::Table.new({"x-delayed-exchange" => true})
        s.vhosts["/"].declare_exchange("bindings_delayed", "topic", true, false, arguments: args)
        internal_q = "amq.delayed-bindings_delayed"
        s.vhosts["/"].queue?(internal_q).should_not be_nil
        body = %({ "routing_key": "#" })
        response = http.post("/api/bindings/%2f/e/bindings_delayed/q/#{internal_q}", body: body)
        response.status_code.should eq 403
        bindings = http.get("/api/bindings/%2f/e/bindings_delayed/q/#{internal_q}")
        JSON.parse(bindings.body).as_a.should be_empty
      end
    end

    it "should refuse binding an internal queue to any exchange" do
      with_http_server do |http, s|
        args = LavinMQ::AMQP::Table.new({"x-delayed-exchange" => true})
        s.vhosts["/"].declare_exchange("bindings_delayed2", "topic", true, false, arguments: args)
        internal_q = "amq.delayed-bindings_delayed2"
        s.vhosts["/"].queue?(internal_q).should_not be_nil
        body = %({ "routing_key": "rk" })
        response = http.post("/api/bindings/%2f/e/amq.direct/q/#{internal_q}", body: body)
        response.status_code.should eq 403
        bindings = http.get("/api/bindings/%2f/e/amq.direct/q/#{internal_q}")
        JSON.parse(bindings.body).as_a.should be_empty
      end
    end

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
    it "lists subscriptions first among the bindings" do
      with_http_server do |http, s|
        declare_mqtt_subscription(s, "mqtt.listed", "a/b", 1u8)
        s.vhosts["/"].declare_queue("q1", false, false)
        s.vhosts["/"].bind_queue("q1", "amq.topic", "rk")
        {http.get("/api/bindings"), http.get("/api/bindings/%2f")}.each do |response|
          response.status_code.should eq 200
          JSON.parse(response.body).as_a.map { |b| {b["source"].as_s, b["destination"].as_s} }
            .should eq [{LavinMQ::MQTT::EXCHANGE, "mqtt.listed"}, {"amq.topic", "q1"}]
        end
      end
    end

    it "lists, adds and removes a session's subscriptions" do
      with_http_server do |http, s|
        declare_mqtt_subscription(s, "mqtt.sub", "a", 1u8)
        x = LavinMQ::MQTT::EXCHANGE
        {"/api/exchanges/%2f/#{x}/bindings/source", "/api/bindings/%2f/e/#{x}/q/mqtt.sub"}.each do |path|
          response = http.get(path)
          response.status_code.should eq 200
          JSON.parse(response.body).as_a.map(&.["routing_key"]).should eq ["a"]
        end

        response = http.post("/api/bindings/%2f/e/#{x}/q/mqtt.sub",
          body: %({"routing_key": "b/#", "arguments": {"mqtt.qos": 0}}))
        response.status_code.should eq 201
        session = s.vhosts["/"].mqtt.session("mqtt.sub")
        subscriptions = s.vhosts["/"].mqtt.subscriptions(session)
        subscriptions.map { |b| {b.routing_key, b.binding_key.qos} }.sort!.should eq [{"a", 1u8}, {"b/#", 0u8}]

        props = URI.encode_path_segment(subscriptions.find!(&.routing_key.== "b/#").binding_key.properties_key)
        http.get("/api/bindings/%2f/e/#{x}/q/mqtt.sub/#{props}").status_code.should eq 200
        http.delete("/api/bindings/%2f/e/#{x}/q/mqtt.sub/#{props}").status_code.should eq 204
        s.vhosts["/"].mqtt.subscriptions(session).map(&.routing_key).should eq ["a"]
      end
    end

    it "needs read on it and write on the session to change subscriptions" do
      with_http_server do |http, s|
        declare_mqtt_subscription(s, "mqtt.sub", "a", 1u8)
        session = s.vhosts["/"].mqtt.session("mqtt.sub")
        props = URI.encode_path_segment(s.vhosts["/"].mqtt.subscriptions(session).first.binding_key.properties_key)
        x = LavinMQ::MQTT::EXCHANGE
        {
          "noread"  => {/^$/, /.*/, "User doesn't have read permissions to exchange '#{x}'"},
          "nowrite" => {/.*/, /^$/, "User doesn't have write permissions to queue 'mqtt.sub'"},
        }.each do |name, (read, write, reason)|
          s.users.create(name, "pw", [LavinMQ::Tag::Management])
          s.users.add_permission(name, "/", /.*/, read, write)
          hdrs = HTTP::Headers{"Authorization" => "Basic #{Base64.strict_encode("#{name}:pw")}"}
          {
            http.post("/api/bindings/%2f/e/#{x}/q/mqtt.sub", headers: hdrs, body: %({"routing_key": "b"})),
            http.delete("/api/bindings/%2f/e/#{x}/q/mqtt.sub/#{props}", headers: hdrs),
          }.each do |response|
            response.status_code.should eq 403
            JSON.parse(response.body)["reason"].should eq reason
          end
        end
        s.vhosts["/"].mqtt.subscriptions(session).map(&.routing_key).should eq ["a"]
      end
    end

    it "can't be bound to an AMQP queue" do
      with_http_server do |http, s|
        v = s.vhosts["/"]
        v.declare_queue("q1", false, false)
        response = http.post("/api/bindings/%2f/e/#{LavinMQ::MQTT::EXCHANGE}/q/q1", body: %({"routing_key": "a"}))
        response.status_code.should eq 403
        v.queue_bindings(v.queue("q1")).map(&.source).should eq [""]
      end
    end

    it "has no bindings to or from exchanges" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("be1", "topic", false, false)
        x = LavinMQ::MQTT::EXCHANGE
        binding = %({"routing_key": "b", "arguments": {}})
        {
          "GET destination"   => {http.get("/api/exchanges/%2f/#{x}/bindings/destination"), 200},
          "GET e/e source"    => {http.get("/api/bindings/%2f/e/#{x}/e/be1"), 200},
          "GET e/e dest"      => {http.get("/api/bindings/%2f/e/be1/e/#{x}"), 200},
          "GET e/e missing"   => {http.get("/api/bindings/%2f/e/#{x}/e/missing"), 404},
          "POST e/e source"   => {http.post("/api/bindings/%2f/e/#{x}/e/be1", body: binding), 403},
          "POST e/e dest"     => {http.post("/api/bindings/%2f/e/be1/e/#{x}", body: binding), 400},
          "GET e/e props"     => {http.get("/api/bindings/%2f/e/#{x}/e/be1/b"), 404},
          "DELETE e/e source" => {http.delete("/api/bindings/%2f/e/#{x}/e/be1/b"), 404},
          "DELETE e/e dest"   => {http.delete("/api/bindings/%2f/e/be1/e/#{x}/b"), 400},
        }.each do |route, (response, status)|
          # The route is in the tuple so a failure says which one it was.
          {route, response.status_code}.should eq({route, status})
          JSON.parse(response.body).as_a.should be_empty if status == 200
        end
        s.vhosts["/"].exchange("be1").bindings_details.should be_empty
      end
    end
  end

  describe "an mqtt session" do
    it "can't be bound from an AMQP exchange" do
      with_http_server do |http, s|
        declare_mqtt_subscription(s, "mqtt.sub", "a", 1u8)
        response = http.post("/api/bindings/%2f/e/amq.topic/q/mqtt.sub",
          body: %({"routing_key": "b", "arguments": {}}))
        response.status_code.should eq 403
        s.vhosts["/"].exchange("amq.topic").bindings_details.should be_empty
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
