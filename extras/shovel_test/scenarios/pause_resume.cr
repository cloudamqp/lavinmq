require "../scenario"

module ShovelTest
  # A paused shovel moves nothing and holds nothing in flight; a resumed one
  # picks up where it left off.
  class PauseResume < Scenario
    COUNT = 50

    def slug : String
      "pause-resume"
    end

    def run : Nil
      src = queue("src")
      dest = queue("dest")
      broker.publish(src, COUNT)
      name = shovel("shovel", shovel_config(src, dest_uri: cfg.amqp_url, dest_queue: dest))
      eventually("the first batch to be moved") { api.queue(dest).ready == COUNT }

      api.pause_shovel(name)
      eventually("the shovel to pause") { status(name).state?("paused") }
      broker.publish(src, COUNT)
      steady("nothing moving while paused", 2.seconds) do
        api.queue(dest).ready == COUNT && api.queue(src).ready == COUNT && api.queue(src).unacked == 0
      end

      api.resume_shovel(name)
      eventually("the second batch to be moved after resume") { api.queue(dest).ready == 2 * COUNT }
      eventually("the source to be empty") { api.queue(src).total == 0 }
      expect(status(name).state?("running"), "resumed shovel should be running, was #{status(name).state}")
    end
  end
end
