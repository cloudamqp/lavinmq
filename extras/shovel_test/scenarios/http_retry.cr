require "../scenario"

module ShovelTest
  # A 5xx response is Retry: the message is requeued and retried with backoff
  # until the endpoint recovers; nothing is lost and nothing is duplicated.
  class HttpRetry < Scenario
    FAILURES = 3

    def slug : String
      "http-retry"
    end

    def run : Nil
      path = "/flaky/503/#{FAILURES}"
      src = queue("src")
      broker.publish(src, 1)
      name = shovel("shovel", shovel_config(src, dest_uri: endpoint_url(path)))

      # Backoff between attempts is 0.5s, 1s, 2s: a few seconds all in all.
      eventually("the endpoint to see #{FAILURES} failed attempts and one success") { hits(path) >= FAILURES + 1 }
      eventually("the source to be empty") { api.queue(src).total == 0 }
      expect(hits(path) == FAILURES + 1, "endpoint should have seen exactly #{FAILURES + 1} attempts, saw #{hits(path)}")
      shovel = status(name)
      expect(shovel.retried == FAILURES, "shovel should report #{FAILURES} retried, reported #{shovel.retried}")
      expect(shovel.confirmed == 1, "shovel should report 1 confirmed, reported #{shovel.confirmed}")
      expect(shovel.state?("running"), "shovel should still be running, was #{shovel.state} (#{shovel.error})")
    end
  end
end
