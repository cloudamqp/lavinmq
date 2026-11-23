require "../scenario"

module ShovelTest
  # https://github.com/cloudamqp/lavinmq/issues/1357
  #
  # A Destination queue that is full, with x-overflow reject-publish, nacks
  # every publish. The nack is a Retry outcome: the Shovel must requeue those
  # messages on the Source and back off, not ack them into the void. Before
  # the fix the whole Source drained while the Destination stayed capped.
  class RejectPublishOverflow < Scenario
    MAX_LENGTH = 100
    # The shovel's default src-prefetch-count: how many messages may be in
    # flight (delivered but not yet requeued) at any moment.
    PREFETCH = 1000

    def slug : String
      "reject-publish-overflow"
    end

    def run : Nil
      total = cfg.messages
      src = queue("src")
      dest = queue("dest", {"x-max-length" => MAX_LENGTH, "x-overflow" => "reject-publish"})
      broker.publish(src, total)
      name = shovel("shovel", shovel_config(src, dest_uri: cfg.amqp_url, dest_queue: dest))

      eventually("the destination to fill up to its max-length") { api.queue(dest).ready == MAX_LENGTH }
      # From here every publish is nacked. The source must keep what is left,
      # give or take the window of messages bouncing between delivery and
      # requeue, and the destination must stay capped.
      steady("the source keeping its messages while the destination is full", 5.seconds) do
        api.queue(dest).total == MAX_LENGTH && api.queue(src).total >= total - MAX_LENGTH - PREFETCH
      end
      shovel = status(name)
      expect(shovel.state?("running"), "shovel should still be running, was #{shovel.state} (#{shovel.error})")
      expect(shovel.retried > 0, "shovel should report retried deliveries, reported #{shovel.retried}")

      # With the shovel gone nothing is in flight, so the books must balance
      # exactly: every message is either on the destination or back on the source.
      api.delete_shovel(name)
      eventually("every in-flight message to return to the source") do
        q = api.queue(src)
        q.unacked == 0 && q.ready == total - MAX_LENGTH
      end
      expect(api.queue(dest).total == MAX_LENGTH, "destination should hold exactly #{MAX_LENGTH} messages")
    end
  end
end
