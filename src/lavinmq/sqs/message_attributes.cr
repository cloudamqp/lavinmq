require "json"
require "base64"
require "digest/md5"
require "amq-protocol"
require "./errors"

module LavinMQ
  module SQS
    # A typed SQS message attribute. Stored losslessly in the AMQP headers
    # table under `x-sqs-attributes` as a nested table:
    #
    #   {"<name>" => {"DataType" => "String", "StringValue" => "..."}}
    #   {"<name>" => {"DataType" => "Binary", "BinaryValue" => Bytes}}
    struct MessageAttribute
      NAME_PATTERN      = /\A[a-zA-Z0-9_\-.]{1,256}\z/
      DATA_TYPE_PATTERN = /\A(String|Number|Binary)(\.[a-zA-Z0-9_\-.]{1,249})?\z/
      RESERVED_PREFIXES = {"aws.", "amazon."}
      MAX_COUNT         = 10

      getter name : String
      getter data_type : String
      getter string_value : String?
      getter binary_value : Bytes?

      def initialize(@name, @data_type, @string_value : String? = nil, @binary_value : Bytes? = nil)
      end

      def binary? : Bool
        @data_type.starts_with?("Binary")
      end

      def value_bytesize : Int32
        if b = @binary_value
          b.bytesize
        else
          @string_value.try(&.bytesize) || 0
        end
      end

      # Size as counted against MaximumMessageSize
      def bytesize : Int32
        @name.bytesize + @data_type.bytesize + value_bytesize
      end

      def self.validate_name!(name : String, param : String) : Nil
        unless name.matches?(NAME_PATTERN)
          raise InvalidParameterValue.new("#{param} name '#{name}' is invalid. Reason: Can only include alphanumeric characters, hyphens, underscores or periods. 1 to 256 in length.")
        end
        downcased = name.downcase
        if RESERVED_PREFIXES.any? { |p| downcased.starts_with?(p) }
          raise InvalidParameterValue.new("#{param} name '#{name}' is invalid. Reason: Names starting with 'AWS.' or 'Amazon.' are reserved.")
        end
        if name.starts_with?('.') || name.ends_with?('.') || name.includes?("..")
          raise InvalidParameterValue.new("#{param} name '#{name}' is invalid. Reason: Can't start or end with a period, or have periods in succession.")
        end
      end

      # Parses the `MessageAttributes` (or `MessageSystemAttributes`) map of a
      # JSON request. Returned sorted by name, which is the order the MD5
      # checksum is computed in.
      def self.parse_all(map : Hash(String, JSON::Any)?, param = "MessageAttributes") : Array(MessageAttribute)
        return Array(MessageAttribute).new(0) if map.nil? || map.empty?
        if map.size > MAX_COUNT
          raise InvalidParameterValue.new("Number of message attributes [#{map.size}] exceeds the allowed maximum [#{MAX_COUNT}].")
        end
        attrs = map.map do |name, value|
          validate_name!(name, param)
          parse(name, value, param)
        end
        attrs.sort_by!(&.name)
      end

      def self.parse(name : String, value : JSON::Any, param : String) : MessageAttribute
        hash = value.as_h? || raise InvalidParameterValue.new("#{param} '#{name}' is invalid. Reason: Must be a map.")
        data_type = hash["DataType"]?.try(&.as_s?) ||
                    raise InvalidParameterValue.new("The message attribute '#{name}' must contain a non-empty message attribute type.")
        unless data_type.matches?(DATA_TYPE_PATTERN)
          raise InvalidParameterValue.new("The type of message attribute '#{name}' is invalid. You must use only the following supported type prefixes: Binary, Number, String.")
        end
        string_value = hash["StringValue"]?.try(&.as_s?)
        binary_value = hash["BinaryValue"]?.try(&.as_s?)
        if data_type.starts_with?("Binary")
          raise InvalidParameterValue.new("The message attribute '#{name}' with type 'Binary' must use field 'Binary'.") if binary_value.nil? || string_value
          bytes = Base64.decode(binary_value) rescue raise InvalidParameterValue.new("The message attribute '#{name}' has an invalid base64 encoded binary value.")
          new(name, data_type, nil, bytes)
        else
          if string_value.nil? || binary_value
            raise InvalidParameterValue.new("The message attribute '#{name}' with type '#{data_type}' must use field 'String'.")
          end
          if string_value.empty?
            raise InvalidParameterValue.new("Message attribute '#{name}' must contain a non-empty value of type '#{data_type}'.")
          end
          new(name, data_type, string_value, nil)
        end
      end

      def to_field : AMQ::Protocol::Table
        table = AMQ::Protocol::Table.new
        table["DataType"] = @data_type
        if b = @binary_value
          table["BinaryValue"] = b
        else
          table["StringValue"] = @string_value
        end
        table
      end

      def self.from_field(name : String, field : AMQ::Protocol::Field) : MessageAttribute?
        table = field.as?(AMQ::Protocol::Table) || return
        data_type = table["DataType"]?.as?(String) || return
        case value = table["BinaryValue"]? || table["StringValue"]?
        when Bytes  then new(name, data_type, nil, value)
        when String then new(name, data_type, value, nil)
        end
      end

      def self.to_table(attrs : Array(MessageAttribute)) : AMQ::Protocol::Table
        table = AMQ::Protocol::Table.new
        attrs.each { |a| table[a.name] = a.to_field }
        table
      end

      def self.from_table(table : AMQ::Protocol::Field?) : Array(MessageAttribute)
        table = table.as?(AMQ::Protocol::Table) || return Array(MessageAttribute).new(0)
        attrs = Array(MessageAttribute).new
        table.each do |name, field|
          if attr = from_field(name, field)
            attrs << attr
          end
        end
        attrs.sort_by!(&.name)
      end

      def to_json(json : JSON::Builder) : Nil
        json.object do
          json.field "DataType", @data_type
          if b = @binary_value
            json.field "BinaryValue", Base64.strict_encode(b)
          else
            json.field "StringValue", @string_value
          end
        end
      end

      # Matches a `MessageAttributeNames` request entry: "All", ".*",
      # "prefix.*" or an exact name.
      def self.selected?(name : String, selectors : Array(String)) : Bool
        selectors.any? do |s|
          s == "All" || s == ".*" || s == name || (s.ends_with?(".*") && name.starts_with?(s.rchop(".*")))
        end
      end
    end

    module Checksums
      def self.md5_hex(bytes : Bytes) : String
        Digest::MD5.hexdigest(bytes)
      end

      def self.md5_hex(string : String) : String
        Digest::MD5.hexdigest(string)
      end

      # MD5OfMessageAttributes as specified by SQS: attributes sorted by name,
      # each encoded as length-prefixed name, length-prefixed data type, one
      # transport byte (1 = string, 2 = binary) and length-prefixed value, with
      # all lengths as 4 byte big endian integers.
      def self.md5_attributes(attrs : Array(MessageAttribute)) : String?
        return if attrs.empty?
        digest = Digest::MD5.new
        buf = uninitialized UInt8[4]
        attrs.each do |attr|
          update_with_length(digest, buf, attr.name.to_slice)
          update_with_length(digest, buf, attr.data_type.to_slice)
          if b = attr.binary_value
            digest.update(Bytes[2_u8])
            update_with_length(digest, buf, b)
          else
            digest.update(Bytes[1_u8])
            update_with_length(digest, buf, (attr.string_value || "").to_slice)
          end
        end
        digest.hexfinal
      end

      private def self.update_with_length(digest, buf, bytes : Bytes) : Nil
        IO::ByteFormat::BigEndian.encode(bytes.bytesize.to_u32, buf.to_slice)
        digest.update(buf.to_slice)
        digest.update(bytes)
      end
    end
  end
end
