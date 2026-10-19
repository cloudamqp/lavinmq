module LavinMQ
  module Endpoint
    # When a message is settled on the endpoint it was consumed from:
    #   OnConfirm - once the destination broker confirms it
    #   OnPublish - once it has been published to the destination
    #   NoAck     - never, it's consumed without acknowledgements
    enum AckMode
      OnConfirm
      OnPublish
      NoAck

      # Parses "on-confirm", "on-publish" or "no-ack"
      def self.from_config?(value : String?) : self?
        parse?(value.to_s.delete("-"))
      end
    end
  end
end
