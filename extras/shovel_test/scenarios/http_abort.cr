require "../scenario"

module ShovelTest
  # A 404 response is Abort: the Destination is unusable. The message is kept
  # on the Source and, after ABORT_THRESHOLD consecutive Aborts, the shovel
  # stops in the `aborted` state for an operator. Once resumed it delivers
  # the kept message.
  class HttpAbort < Scenario
    ABORT_THRESHOLD = 10

    def slug : String
      "http-abort"
    end

    def run : Nil
      # 404 exactly as many times as it takes to abort, then healthy again.
      path = "/flaky/404/#{ABORT_THRESHOLD}"
      src = queue("src")
      broker.publish(src, 1)
      name = shovel("shovel", shovel_config(src, dest_uri: endpoint_url(path)))

      eventually("the shovel to abort") { status(name).state?("aborted") }
      shovel = status(name)
      expect(shovel.error.to_s.includes?("unusable"), "aborted shovel should say why, error was #{shovel.error.inspect}")
      expect(shovel.aborted == ABORT_THRESHOLD, "shovel should report #{ABORT_THRESHOLD} aborted, reported #{shovel.aborted}")
      expect(hits(path) == ABORT_THRESHOLD, "endpoint should have seen #{ABORT_THRESHOLD} attempts, saw #{hits(path)}")
      eventually("the message to stay on the source, not in flight") do
        q = api.queue(src)
        q.ready == 1 && q.unacked == 0
      end

      api.resume_shovel(name)
      eventually("the resumed shovel to deliver the kept message") { api.queue(src).total == 0 }
      shovel = status(name)
      expect(shovel.state?("running"), "resumed shovel should be running, was #{shovel.state} (#{shovel.error})")
      expect(shovel.confirmed == 1, "resumed shovel should report 1 confirmed, reported #{shovel.confirmed}")
    end
  end
end
