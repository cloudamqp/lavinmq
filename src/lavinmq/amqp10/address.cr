module LavinMQ::AMQP10
  record PublishAddress, exchange : String, routing_key : String

  module Address
    extend self

    def parse_target(address : String) : PublishAddress?
      if component = queue_component(address)
        PublishAddress.new("", component)
      elsif exchange = exchange_component(address)
        PublishAddress.new(exchange[0], exchange[1])
      end
    end

    def parse_source(address : String) : String?
      queue_component(address)
    end

    private def queue_component(address : String) : String?
      prefix = "/queues/"
      return unless address.starts_with?(prefix)
      encoded = address[prefix.bytesize..]?
      return if encoded.nil? || encoded.empty? || encoded.includes?('/')
      percent_decode(encoded)
    end

    private def exchange_component(address : String) : Tuple(String, String)?
      prefix = "/exchanges/"
      return unless address.starts_with?(prefix)
      rest = address[prefix.bytesize..]?
      return if rest.nil? || rest.empty?
      slash = rest.index('/')
      if slash
        exchange = rest[0, slash]
        routing_key = rest[(slash + 1)..]
      else
        exchange = rest
        routing_key = ""
      end
      return if exchange.empty?
      {percent_decode(exchange), percent_decode(routing_key)}
    end

    private def percent_decode(value : String) : String
      return value unless value.includes?('%')
      String.build(value.bytesize) do |io|
        i = 0
        while i < value.bytesize
          byte = value.byte_at(i)
          if byte == '%'.ord && i + 2 < value.bytesize
            hi = hex(value.byte_at(i + 1))
            lo = hex(value.byte_at(i + 2))
            if hi && lo
              io.write_byte(((hi << 4) | lo).to_u8)
              i += 3
              next
            end
          end
          io.write_byte(byte)
          i += 1
        end
      end
    end

    private def hex(byte : UInt8) : UInt8?
      case byte
      when '0'.ord..'9'.ord then (byte - '0'.ord).to_u8
      when 'A'.ord..'F'.ord then (byte - 'A'.ord + 10).to_u8
      when 'a'.ord..'f'.ord then (byte - 'a'.ord + 10).to_u8
      end
    end
  end
end
