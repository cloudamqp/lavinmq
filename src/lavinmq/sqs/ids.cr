require "uuid"
require "base64"
require "random/secure"

module LavinMQ
  module SQS
    # Unpredictable ids from a buffer of secure random bytes refilled in bulk,
    # so request ids, message ids and receipt handles don't cost a getrandom
    # syscall each.
    module Ids
      @@buffer = Bytes.new(4096)
      @@pos = 4096
      @@lock = Mutex.new

      private def self.take(n : Int32, & : Bytes -> T) : T forall T
        @@lock.synchronize do
          if @@pos + n > @@buffer.size
            Random::Secure.random_bytes(@@buffer)
            @@pos = 0
          end
          slice = @@buffer[@@pos, n]
          @@pos += n
          yield slice
        end
      end

      def self.uuid : String
        take(16) do |bytes|
          uuid = UUID.new(bytes, variant: UUID::Variant::RFC4122, version: UUID::Version::V4)
          uuid.to_s
        end
      end

      def self.handle : String
        take(32) { |bytes| Base64.urlsafe_encode(bytes, padding: false) }
      end
    end
  end
end
