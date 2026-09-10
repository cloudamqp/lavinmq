require "../scenario"

module ShovelTest
  # A 400 response is Reject: the message itself is unacceptable. It is
  # rejected without requeue, so the Source queue's dead-letter exchange takes
  # it, and the shovel carries on.
  class HttpReject < Scenario
    def slug : String
      "http-reject"
    end

    def run : Nil
      dlq = queue("dlq")
      src = queue("src", {"x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => dlq})
      broker.publish(src, 1, body: "unacceptable")
      name = shovel("shovel", shovel_config(src, dest_uri: endpoint_url("/400")))

      eventually("the message to be dead-lettered") { api.queue(dlq).ready == 1 }
      expect(api.queue(src).total == 0, "source should be empty once the message is dead-lettered")
      expect(hits("/400") == 1, "endpoint should have been asked exactly once, was asked #{hits("/400")} times")
      shovel = status(name)
      expect(shovel.rejected == 1, "shovel should report 1 rejected, reported #{shovel.rejected}")
      expect(shovel.state?("running"), "shovel should still be running, was #{shovel.state} (#{shovel.error})")
    end
  end
end
