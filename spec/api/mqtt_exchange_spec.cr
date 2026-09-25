require "../spec_helper"

describe LavinMQ::HTTP::MQTTExchangeController do
  x = LavinMQ::MQTT::EXCHANGE

  it "refuses every route that would manage the exchange" do
    with_http_server do |http, s|
      s.vhosts["/"].declare_exchange("be1", "topic", false, false)
      binding = %({"routing_key": "a", "arguments": {}})
      {
        "PUT exchange"      => http.put("/api/exchanges/%2f/#{x}", body: %({"type": "topic"})),
        "DELETE exchange"   => http.delete("/api/exchanges/%2f/#{x}"),
        "POST publish"      => http.post("/api/exchanges/%2f/#{x}/publish", body: "{}"),
        "POST e/q"          => http.post("/api/bindings/%2f/e/#{x}/q/mqtt.sub", body: binding),
        "DELETE e/q"        => http.delete("/api/bindings/%2f/e/#{x}/q/mqtt.sub/a~1"),
        "POST e/e source"   => http.post("/api/bindings/%2f/e/#{x}/e/be1", body: binding),
        "DELETE e/e source" => http.delete("/api/bindings/%2f/e/#{x}/e/be1/a"),
        "POST e/e dest"     => http.post("/api/bindings/%2f/e/be1/e/#{x}", body: binding),
        "DELETE e/e dest"   => http.delete("/api/bindings/%2f/e/be1/e/#{x}/a"),
      }.each do |route, response|
        # The route is in the tuple so a failure says which one it was.
        {route, response.status_code}.should eq({route, 501})
        JSON.parse(response.body)["error"].should eq "not_implemented"
      end
    end
  end

  it "leaves the exchange untouched when a route is refused" do
    with_http_server do |http, s|
      mqtt_args = LavinMQ::AMQP::Table.new({"x-queue-type" => "mqtt"})
      s.vhosts["/"].declare_queue("mqtt.sub", true, false, mqtt_args)

      http.post("/api/bindings/%2f/e/#{x}/q/mqtt.sub",
        body: %({"routing_key": "a", "arguments": {}}))
      s.vhosts["/"].mqtt_exchange.binding_count.should eq 0
      s.vhosts["/"].mqtt_exchange.name.should eq x
    end
  end

  it "still serves the GET routes through the ordinary controllers" do
    with_http_server do |http, s|
      mqtt_args = LavinMQ::AMQP::Table.new({"x-queue-type" => "mqtt"})
      s.vhosts["/"].declare_queue("mqtt.get", true, false, mqtt_args)
      s.vhosts["/"].bind_queue("mqtt.get", x, "a/b", LavinMQ::MQTT.qos_arguments(1u8))

      response = http.get("/api/exchanges/%2f/#{x}/bindings/source")
      response.status_code.should eq 200
      JSON.parse(response.body).as_a.map(&.["destination"]).should eq ["mqtt.get"]

      response = http.get("/api/bindings/%2f/e/#{x}/q/mqtt.get")
      response.status_code.should eq 200
      JSON.parse(response.body).as_a.size.should eq 1
    end
  end
end
