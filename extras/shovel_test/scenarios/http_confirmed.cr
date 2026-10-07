require "../scenario"

module ShovelTest
  # A 2xx response from the HTTP Destination is Confirmed: the message is
  # acked on the Source and counted as confirmed.
  class HttpConfirmed < Scenario
    COUNT = 200

    def slug : String
      "http-confirmed"
    end

    def run : Nil
      src = queue("src")
      broker.publish(src, COUNT, content_type: "text/plain")
      name = shovel("shovel", shovel_config(src, dest_uri: endpoint_url("/200")))

      eventually("the endpoint to receive every message") { hits("/200") >= COUNT }
      eventually("the source to be empty") { api.queue(src).total == 0 }
      expect(hits("/200") == COUNT, "endpoint should have received exactly #{COUNT} requests, got #{hits("/200")}")
      shovel = status(name)
      expect(shovel.confirmed == COUNT, "shovel should report #{COUNT} confirmed, reported #{shovel.confirmed}")
      expect(shovel.state?("running"), "shovel should still be running, was #{shovel.state} (#{shovel.error})")
    end
  end
end
