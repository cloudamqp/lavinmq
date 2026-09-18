require "./codec"

module LavinMQ::AMQP10
  module Descriptor
    SASL_MECHANISMS = 0x40_u64
    SASL_INIT       = 0x41_u64
    SASL_OUTCOME    = 0x44_u64

    OPEN        = 0x10_u64
    BEGIN       = 0x11_u64
    ATTACH      = 0x12_u64
    FLOW        = 0x13_u64
    TRANSFER    = 0x14_u64
    DISPOSITION = 0x15_u64
    DETACH      = 0x16_u64
    END         = 0x17_u64
    CLOSE       = 0x18_u64
    ERROR       = 0x1d_u64

    SOURCE = 0x28_u64
    TARGET = 0x29_u64

    ACCEPTED = 0x24_u64
    REJECTED = 0x25_u64
    RELEASED = 0x26_u64
    MODIFIED = 0x27_u64

    HEADER                 = 0x70_u64
    DELIVERY_ANNOTATIONS   = 0x71_u64
    MESSAGE_ANNOTATIONS    = 0x72_u64
    PROPERTIES             = 0x73_u64
    APPLICATION_PROPERTIES = 0x74_u64
    DATA                   = 0x75_u64
    AMQP_VALUE             = 0x77_u64
    FOOTER                 = 0x78_u64
  end

  module ErrorCondition
    INTERNAL_ERROR          = "amqp:internal-error"
    NOT_FOUND               = "amqp:not-found"
    UNAUTHORIZED_ACCESS     = "amqp:unauthorized-access"
    DECODE_ERROR            = "amqp:decode-error"
    RESOURCE_LIMIT_EXCEEDED = "amqp:resource-limit-exceeded"
    NOT_ALLOWED             = "amqp:not-allowed"
    INVALID_FIELD           = "amqp:invalid-field"
    NOT_IMPLEMENTED         = "amqp:not-implemented"
    RESOURCE_LOCKED         = "amqp:resource-locked"
    RESOURCE_DELETED        = "amqp:resource-deleted"
    PRECONDITION_FAILED     = "amqp:precondition-failed"
    ILLEGAL_STATE           = "amqp:illegal-state"
  end

  enum Role
    Sender
    Receiver
  end

  enum Outcome
    Accepted
    Released
    Rejected
    Modified
  end

  record ErrorInfo, condition : String, description : String? = nil do
    # Still used by connection_factory.cr's pre-authentication close path,
    # which sends via the generic Value-based FrameWriter.write_performative
    # (SASL/pre-auth rejection is low-frequency, left as-is). write_to/encoded_size
    # below are for client.cr's direct-write senders.
    def to_value : Value
      fields = Array(Value).new(2)
      fields << Value.symbol(condition)
      if desc = description
        fields << Value.string(desc)
      end
      Value.described(Value.ulong(Descriptor::ERROR), Value.list(fields))
    end

    def encoded_size : Int32
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    def write_to(io : IO) : Nil
      count = 1
      count = 2 if description
      Codec.write_descriptor(io, Descriptor::ERROR)
      Codec.write_list_header(io, fields_size, count)
      Codec.write_symbol(io, condition)
      if desc = description
        Codec.write_string(io, desc)
      end
    end

    private def fields_size : Int32
      size = Codec.string_size(condition)
      if desc = description
        size += Codec.string_size(desc)
      end
      size
    end
  end

  record Source,
    address : String?,
    durable : UInt32 = 0_u32,
    dynamic : Bool = false,
    dynamic_node_properties : Value? = nil,
    filter : Value? = nil do
    def self.from_value(value : Value?) : Source?
      return unless value
      described = value.described? || return
      return unless described.descriptor_code? == Descriptor::SOURCE
      fields = described.value.list? || Array(Value).new
      address = fields[0]?.try &.string_like?
      durable = fields[1]?.try(&.uint?).try(&.to_u32) || 0_u32
      dynamic = fields[4]?.try(&.bool?) || false
      dynamic_node_properties = fields[5]?.try { |v| v.null? ? nil : v }
      filter = fields[7]?.try { |v| v.null? ? nil : v }
      new(address, durable, dynamic, dynamic_node_properties, filter)
    end

    # Still used by spec/amqp10_spec.cr's test-client helper, which builds
    # outgoing Attach frames via the generic Value encoding; write_to/
    # encoded_size below are for client.cr's direct-write sender.
    def to_value : Value
      fields = Array(Value).new(dynamic_node_properties ? 6 : 5)
      fields << (address.try { |a| Value.string(a) } || Value.null)
      fields << (durable.zero? ? Value.null : Value.uint(durable))
      fields << Value.null
      fields << Value.null
      fields << (dynamic ? Value.bool(true) : Value.null)
      if props = dynamic_node_properties
        fields << props
      end
      Value.described(Value.ulong(Descriptor::SOURCE), Value.list(fields))
    end

    def encoded_size : Int32
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    def write_to(io : IO) : Nil
      Codec.write_descriptor(io, Descriptor::SOURCE)
      Codec.write_list_header(io, fields_size, dynamic_node_properties ? 6 : 5)
      Codec.write_nullable_string(io, address)
      if durable.zero?
        io.write_byte 0x40_u8
      else
        Codec.write_uint(io, durable)
      end
      io.write_byte 0x40_u8 # expiry-policy: null
      io.write_byte 0x40_u8 # timeout: null
      io.write_byte(dynamic ? 0x41_u8 : 0x40_u8)
      if props = dynamic_node_properties
        Codec.write_value(io, props)
      end
    end

    private def fields_size : Int32
      size = Codec.nullable_string_size(address)
      size += durable.zero? ? 1 : Codec.uint_size(durable)
      size += 1 + 1 + 1 # expiry-policy, timeout, dynamic
      if props = dynamic_node_properties
        size += Codec.encoded_size(props)
      end
      size
    end
  end

  record Target,
    address : String?,
    durable : UInt32 = 0_u32,
    dynamic : Bool = false,
    dynamic_node_properties : Value? = nil do
    def self.from_value(value : Value?) : Target?
      return unless value
      described = value.described? || return
      return unless described.descriptor_code? == Descriptor::TARGET
      fields = described.value.list? || Array(Value).new
      address = fields[0]?.try &.string_like?
      durable = fields[1]?.try(&.uint?).try(&.to_u32) || 0_u32
      dynamic = fields[4]?.try(&.bool?) || false
      dynamic_node_properties = fields[5]?.try { |v| v.null? ? nil : v }
      new(address, durable, dynamic, dynamic_node_properties)
    end

    # Still used by spec/amqp10_spec.cr's test-client helper, which builds
    # outgoing Attach frames via the generic Value encoding; write_to/
    # encoded_size below are for client.cr's direct-write sender.
    def to_value : Value
      fields = Array(Value).new(dynamic_node_properties ? 6 : 5)
      fields << (address.try { |a| Value.string(a) } || Value.null)
      fields << (durable.zero? ? Value.null : Value.uint(durable))
      fields << Value.null
      fields << Value.null
      fields << (dynamic ? Value.bool(true) : Value.null)
      if props = dynamic_node_properties
        fields << props
      end
      Value.described(Value.ulong(Descriptor::TARGET), Value.list(fields))
    end

    def encoded_size : Int32
      3 + Codec.list_header_size(fields_size) + fields_size
    end

    def write_to(io : IO) : Nil
      Codec.write_descriptor(io, Descriptor::TARGET)
      Codec.write_list_header(io, fields_size, dynamic_node_properties ? 6 : 5)
      Codec.write_nullable_string(io, address)
      if durable.zero?
        io.write_byte 0x40_u8
      else
        Codec.write_uint(io, durable)
      end
      io.write_byte 0x40_u8 # expiry-policy: null
      io.write_byte 0x40_u8 # timeout: null
      io.write_byte(dynamic ? 0x41_u8 : 0x40_u8)
      if props = dynamic_node_properties
        Codec.write_value(io, props)
      end
    end

    private def fields_size : Int32
      size = Codec.nullable_string_size(address)
      size += durable.zero? ? 1 : Codec.uint_size(durable)
      size += 1 + 1 + 1 # expiry-policy, timeout, dynamic
      if props = dynamic_node_properties
        size += Codec.encoded_size(props)
      end
      size
    end
  end

  record Open,
    container_id : String,
    hostname : String?,
    max_frame_size : UInt32 = Config.instance.frame_max,
    idle_time_out : UInt32? = nil do
    def self.from_value(value : Value) : Open
      described = value.described? || raise DecodeError.new("expected open")
      raise DecodeError.new("expected open") unless described.descriptor_code? == Descriptor::OPEN
      fields = described.value.list? || raise DecodeError.new("open fields must be list")
      container_id = fields[0]?.try(&.string_like?) || ""
      hostname = fields[1]?.try &.string_like?
      max_frame_size = fields[2]?.try(&.uint?).try(&.to_u32) || Config.instance.frame_max
      # field 3 is channel-max, field 4 is idle-time-out (milliseconds)
      idle_time_out = fields[4]?.try(&.uint?).try(&.to_u32)
      new(container_id, hostname, max_frame_size, idle_time_out)
    end
  end

  record Begin,
    remote_channel : UInt16? = nil,
    next_outgoing_id : UInt32 = 0_u32,
    incoming_window : UInt32 = DEFAULT_WINDOW,
    outgoing_window : UInt32 = DEFAULT_WINDOW do
    def self.from_value(value : Value) : Begin
      described = value.described? || raise DecodeError.new("expected begin")
      raise DecodeError.new("expected begin") unless described.descriptor_code? == Descriptor::BEGIN
      fields = described.value.list? || Array(Value).new
      remote_channel = fields[0]?.try(&.uint?).try(&.to_u16)
      next_outgoing_id = fields[1]?.try(&.uint?).try(&.to_u32) || 0_u32
      incoming_window = fields[2]?.try(&.uint?).try(&.to_u32) || DEFAULT_WINDOW
      outgoing_window = fields[3]?.try(&.uint?).try(&.to_u32) || DEFAULT_WINDOW
      new(remote_channel, next_outgoing_id, incoming_window, outgoing_window)
    end
  end

  record Attach,
    name : String,
    handle : UInt32,
    role : Role,
    snd_settle_mode : UInt8? = nil,
    rcv_settle_mode : UInt8? = nil,
    source : Source? = nil,
    target : Target? = nil,
    initial_delivery_count : UInt32? = nil do
    def self.from_value(value : Value) : Attach
      described = value.described? || raise DecodeError.new("expected attach")
      raise DecodeError.new("expected attach") unless described.descriptor_code? == Descriptor::ATTACH
      fields = described.value.list? || raise DecodeError.new("attach fields must be list")
      name = fields[0]?.try(&.string_like?) || raise DecodeError.new("attach missing name")
      handle = fields[1]?.try(&.uint?).try(&.to_u32) || raise DecodeError.new("attach missing handle")
      role_bool = fields[2]?.try(&.bool?)
      role = role_bool ? Role::Receiver : Role::Sender
      snd_settle_mode = fields[3]?.try(&.uint?).try(&.to_u8)
      rcv_settle_mode = fields[4]?.try(&.uint?).try(&.to_u8)
      source = Source.from_value(fields[5]?)
      target = Target.from_value(fields[6]?)
      initial_delivery_count = fields[9]?.try(&.uint?).try(&.to_u32)
      new(name, handle, role, snd_settle_mode, rcv_settle_mode, source, target, initial_delivery_count)
    end
  end

  record Flow,
    next_incoming_id : UInt32?,
    incoming_window : UInt32?,
    next_outgoing_id : UInt32?,
    outgoing_window : UInt32?,
    handle : UInt32?,
    delivery_count : UInt32?,
    link_credit : UInt32?,
    available : UInt32?,
    drain : Bool,
    echo : Bool do
    def self.from_value(value : Value) : Flow
      described = value.described? || raise DecodeError.new("expected flow")
      raise DecodeError.new("expected flow") unless described.descriptor_code? == Descriptor::FLOW
      fields = described.value.list? || Array(Value).new
      new(
        fields[0]?.try(&.uint?).try(&.to_u32),
        fields[1]?.try(&.uint?).try(&.to_u32),
        fields[2]?.try(&.uint?).try(&.to_u32),
        fields[3]?.try(&.uint?).try(&.to_u32),
        fields[4]?.try(&.uint?).try(&.to_u32),
        fields[5]?.try(&.uint?).try(&.to_u32),
        fields[6]?.try(&.uint?).try(&.to_u32),
        fields[7]?.try(&.uint?).try(&.to_u32),
        fields[8]?.try(&.bool?) || false,
        fields[9]?.try(&.bool?) || false,
      )
    end
  end

  record Detach, handle : UInt32, closed : Bool do
    def self.from_value(value : Value) : Detach
      described = value.described? || raise DecodeError.new("expected detach")
      raise DecodeError.new("expected detach") unless described.descriptor_code? == Descriptor::DETACH
      fields = described.value.list? || raise DecodeError.new("detach fields must be list")
      handle = fields[0]?.try(&.uint?).try(&.to_u32) || raise DecodeError.new("detach missing handle")
      closed = fields[1]?.try(&.bool?) || false
      new(handle, closed)
    end
  end
end
