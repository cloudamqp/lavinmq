require "socket"

module LavinMQ
  # Matches IP addresses against either exact IPs or CIDR ranges
  struct IPMatcher
    enum Type
      IPv4
      IPv6
    end

    @type : Type
    @network : Bytes
    @mask : Bytes

    def initialize(@type : Type, @network : Bytes, @mask : Bytes)
    end

    # Parse from config string: "192.168.0.0/24", "10.0.0.1", "2001:db8::1/128", or "2001:db8::1"
    def self.parse(source : String) : IPMatcher
      source = source.strip
      if source.includes?('/')
        parts = source.split('/', 2)
        ip_str = parts[0].strip
        prefix_str = parts[1].strip
        prefix = prefix_str.to_u8?
        raise ArgumentError.new("Invalid CIDR prefix: #{prefix_str}") unless prefix
      else
        ip_str = source
        prefix = nil
      end

      if fields = Socket::IPAddress.parse_v4_fields?(ip_str)
        p = prefix || 32_u8
        raise ArgumentError.new("IPv4 prefix must be 0-32, got #{p}") if p > 32
        network = Bytes.new(4) { |i| fields[i] }
        mask = calculate_mask(p, 4)
        network.size.times { |i| network[i] &= mask[i] }
        return new(Type::IPv4, network, mask)
      end

      if fields = Socket::IPAddress.parse_v6_fields?(ip_str)
        p = prefix || 128_u8
        raise ArgumentError.new("IPv6 prefix must be 0-128, got #{p}") if p > 128
        network = v6_fields_to_bytes(fields)
        mask = calculate_mask(p, 16)
        network.size.times { |i| network[i] &= mask[i] }
        return new(Type::IPv6, network, mask)
      end

      raise ArgumentError.new("Invalid IP address: #{ip_str}")
    end

    # Parse a comma-separated list of IP addresses or CIDR ranges from config.
    # Raises on any invalid entry so a misconfigured trusted-source list fails
    # loudly at startup instead of silently collapsing to an empty (trust-all) list.
    def self.parse_list(sources : String) : Array(IPMatcher)
      sources.split(',')
        .map(&.strip)
        .reject(&.empty?)
        .map do |source|
          begin
            parse(source)
          rescue ex : Socket::Error | ArgumentError
            raise ArgumentError.new("Invalid IP/CIDR '#{source}': #{ex.message}")
          end
        end
    end

    # Check if an IP address matches this matcher
    def matches?(address : String) : Bool
      target = case @type
               in Type::IPv4 then ip_to_bytes_v4(address)
               in Type::IPv6 then ip_to_bytes_v6(address)
               end
      return false unless target
      matches?(target, @network, @mask)
    end

    # Calculate netmask from prefix length
    private def self.calculate_mask(prefix : UInt8, total_bytes : Int32) : Bytes
      mask = Bytes.new(total_bytes, 0_u8)
      full_bytes = prefix // 8
      remaining_bits = prefix % 8

      # Set full bytes to 0xFF
      full_bytes.times { |i| mask[i] = 0xFF_u8 }

      # Set partial byte if any remaining bits
      if remaining_bits > 0 && full_bytes < total_bytes
        mask[full_bytes] = (0xFF_u8 << (8 - remaining_bits))
      end

      mask
    end

    # Convert IPv4 address string to bytes
    private def ip_to_bytes_v4(address : String) : Bytes?
      if fields = Socket::IPAddress.parse_v4_fields?(address)
        Bytes.new(4) { |i| fields[i] }
      elsif fields = Socket::IPAddress.parse_v6_fields?(address)
        # On a dual-stack (::) listener an IPv4 peer arrives as an IPv4-mapped
        # IPv6 address (::ffff:a.b.c.d); extract the embedded IPv4 so an IPv4
        # trusted source still matches (issue 2070).
        ipv4_mapped_bytes(fields)
      end
    end

    # If *fields* is an IPv4-mapped IPv6 address (::ffff:a.b.c.d), return the
    # embedded 4-byte IPv4 address, otherwise nil.
    private def ipv4_mapped_bytes(fields : StaticArray(UInt16, 8)) : Bytes?
      return nil unless fields[0].zero? && fields[1].zero? && fields[2].zero? &&
                        fields[3].zero? && fields[4].zero? && fields[5] == 0xffff_u16
      Bytes[
        (fields[6] >> 8).to_u8, (fields[6] & 0xFF).to_u8,
        (fields[7] >> 8).to_u8, (fields[7] & 0xFF).to_u8,
      ]
    end

    # Convert IPv6 address string to bytes
    private def ip_to_bytes_v6(address : String) : Bytes?
      if fields = Socket::IPAddress.parse_v6_fields?(address)
        IPMatcher.v6_fields_to_bytes(fields)
      end
    end

    # Convert IPv6 fields array to bytes (shared helper)
    protected def self.v6_fields_to_bytes(fields : StaticArray(UInt16, 8)) : Bytes
      bytes = Bytes.new(16)
      fields.each_with_index do |field, i|
        bytes[i * 2] = (field >> 8).to_u8
        bytes[i * 2 + 1] = (field & 0xFF).to_u8
      end
      bytes
    end

    private def matches?(target : Bytes, network : Bytes, mask : Bytes) : Bool
      return false unless target.size == network.size == mask.size

      target.size.times do |i|
        return false if (target[i] & mask[i]) != (network[i] & mask[i])
      end

      true
    end
  end
end
