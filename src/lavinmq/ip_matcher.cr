require "socket"

module LavinMQ
  # Matches IP addresses against either exact IPs or CIDR ranges
  struct IPMatcher
    enum Type
      ExactIPv4
      ExactIPv6
      CIDRv4
      CIDRv6
    end

    @type : Type
    @address_string : String
    @network : Bytes? # Only used for CIDR
    @mask : Bytes?    # Only used for CIDR

    def initialize(@type : Type, @address_string : String, @network : Bytes? = nil, @mask : Bytes? = nil)
    end

    # Parse from config string: "192.168.0.0/24" or "10.0.0.1"
    def self.parse(source : String) : IPMatcher
      if source.includes?('/')
        parse_cidr(source)
      else
        parse_exact_ip(source)
      end
    end

    # Check if an IP address matches this matcher
    def matches?(address : String) : Bool
      case @type
      when Type::ExactIPv4, Type::ExactIPv6
        @address_string == address
      when Type::CIDRv4
        network = @network
        mask = @mask
        return false unless network && mask
        if target = ip_to_bytes_v4(address)
          matches_cidr?(target, network, mask)
        else
          false
        end
      when Type::CIDRv6
        network = @network
        mask = @mask
        return false unless network && mask
        if target = ip_to_bytes_v6(address)
          matches_cidr?(target, network, mask)
        else
          false
        end
      else
        false
      end
    end

    private def self.parse_cidr(source : String) : IPMatcher
      parts = source.split('/', 2)
      raise ArgumentError.new("Invalid CIDR notation: #{source}") if parts.size != 2

      ip_str = parts[0].strip
      prefix_str = parts[1].strip
      prefix = prefix_str.to_u8?
      raise ArgumentError.new("Invalid CIDR prefix: #{prefix_str}") unless prefix

      # Try IPv4 first
      if fields = Socket::IPAddress.parse_v4_fields?(ip_str)
        raise ArgumentError.new("IPv4 prefix must be 0-32, got #{prefix}") if prefix > 32
        network = Bytes.new(4) { |i| fields[i] }
        mask = calculate_mask(prefix, 4)
        # Apply mask to network address to normalize it
        network.size.times { |i| network[i] &= mask[i] }
        return new(Type::CIDRv4, source, network, mask)
      end

      # Try IPv6
      if fields = Socket::IPAddress.parse_v6_fields?(ip_str)
        raise ArgumentError.new("IPv6 prefix must be 0-128, got #{prefix}") if prefix > 128
        network = v6_fields_to_bytes(fields)
        mask = calculate_mask(prefix, 16)
        # Apply mask to network address to normalize it
        network.size.times { |i| network[i] &= mask[i] }
        return new(Type::CIDRv6, source, network, mask)
      end

      raise ArgumentError.new("Invalid IP address in CIDR: #{ip_str}")
    end

    private def self.parse_exact_ip(source : String) : IPMatcher
      source = source.strip

      # Try IPv4 - just validate
      if Socket::IPAddress.parse_v4_fields?(source)
        return new(Type::ExactIPv4, source)
      end

      # Try IPv6 - just validate
      if Socket::IPAddress.parse_v6_fields?(source)
        return new(Type::ExactIPv6, source)
      end

      raise ArgumentError.new("Invalid IP address: #{source}")
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
      end
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

    # Check if target IP matches CIDR range using bitwise AND
    private def matches_cidr?(target : Bytes, network : Bytes, mask : Bytes) : Bool
      return false unless target.size == network.size == mask.size

      target.size.times do |i|
        return false if (target[i] & mask[i]) != (network[i] & mask[i])
      end

      true
    end
  end
end
