module LavinMQ
  module Shovel
    module Retrier
      struct Either
        getter error
        getter data
      end

      def self.push_with_retry(max_retries : Int32, jitter : Float64, backoff : Float64, & : -> Bool) : Bool
        retries = 0
        while retries <= max_retries
          return true if yield
          retries += 1
          base_delay = (backoff ** retries).seconds
          jitter_delay = jitter.seconds * Random.rand(0.0..1.0)
          sleep base_delay + jitter_delay
        end
        false
      end
    end
  end
end
