module LavinMQ
  module Shovel
    module Retrier
      def self.push_with_retry(max_retries : Int32, jitter : Float64, backoff : Float64, & : -> Bool) : Bool
        retries = 0
        loop do
          return true if yield
          retries += 1
          break if retries > max_retries
          base_delay = (backoff ** retries).seconds
          jitter_delay = jitter.seconds * Random.rand(0.0..1.0)
          sleep base_delay + jitter_delay
        end
        false
      end
    end
  end
end
