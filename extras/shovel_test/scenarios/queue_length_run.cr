require "../scenario"

module ShovelTest
  # A queue-length run moves as many messages as the Source held when the
  # shovel started, then deletes its own parameter.
  class QueueLengthRun < Scenario
    def slug : String
      "queue-length-run"
    end

    def run : Nil
      total = cfg.messages
      src = queue("src")
      dest = queue("dest")
      broker.publish(src, total)
      name = shovel("shovel", shovel_config(src, dest_uri: cfg.amqp_url, dest_queue: dest, src_delete_after: "queue-length"))

      eventually("every message to reach the destination") { api.queue(dest).ready >= total }
      eventually("the source to be empty") { api.queue(src).total == 0 }
      expect(api.queue(dest).ready == total, "destination should hold exactly #{total} messages, holds #{api.queue(dest).ready}")
      eventually("the shovel to delete its own parameter") { !api.shovel_parameter?(name) }
    end
  end
end
