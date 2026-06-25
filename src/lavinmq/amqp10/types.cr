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
    PRECONDITION_FAILED     = "amqp:precondition-failed"
    ILLEGAL_STATE           = "amqp:illegal-state"
  end

  enum Role
    Sender
    Receiver
  end

  enum LinkKind
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
    def to_value : Value
      fields = Array(Value).new(2)
      fields << Value.symbol(condition)
      if desc = description
        fields << Value.string(desc)
      end
      Value.described(Value.ulong(Descriptor::ERROR), Value.list(fields))
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
  end

  record Open,
    container_id : String,
    hostname : String?,
    max_frame_size : UInt32 = Config.instance.frame_max do
    def self.from_value(value : Value) : Open
      described = value.described? || raise DecodeError.new("expected open")
      raise DecodeError.new("expected open") unless described.descriptor_code? == Descriptor::OPEN
      fields = described.value.list? || raise DecodeError.new("open fields must be list")
      container_id = fields[0]?.try(&.string_like?) || ""
      hostname = fields[1]?.try &.string_like?
      max_frame_size = fields[2]?.try(&.uint?).try(&.to_u32) || Config.instance.frame_max
      new(container_id, hostname, max_frame_size)
    end
  end

  record Begin, remote_channel : UInt16? = nil do
    def self.from_value(value : Value) : Begin
      described = value.described? || raise DecodeError.new("expected begin")
      raise DecodeError.new("expected begin") unless described.descriptor_code? == Descriptor::BEGIN
      fields = described.value.list? || Array(Value).new
      remote_channel = fields[0]?.try(&.uint?).try(&.to_u16)
      new(remote_channel)
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

  record Transfer,
    handle : UInt32,
    delivery_id : UInt32?,
    delivery_tag : Bytes?,
    message_format : UInt32?,
    settled : Bool,
    more : Bool,
    state : Value?,
    aborted : Bool do
    def self.decode(reader : SliceReader) : Transfer
      value = Codec.decode(reader)
      described = value.described? || raise DecodeError.new("expected transfer")
      raise DecodeError.new("expected transfer") unless described.descriptor_code? == Descriptor::TRANSFER
      fields = described.value.list? || raise DecodeError.new("transfer fields must be list")
      handle = fields[0]?.try(&.uint?).try(&.to_u32) || raise DecodeError.new("transfer missing handle")
      delivery_id = fields[1]?.try(&.uint?).try(&.to_u32)
      delivery_tag = fields[2]?.try &.binary?
      message_format = fields[3]?.try(&.uint?).try(&.to_u32)
      settled = fields[4]?.try(&.bool?) || false
      more = fields[5]?.try(&.bool?) || false
      state = fields[7]?
      aborted = fields[9]?.try(&.bool?) || false
      new(handle, delivery_id, delivery_tag, message_format, settled, more, state, aborted)
    end
  end

  record Disposition,
    role : Role,
    first : UInt32,
    last : UInt32?,
    settled : Bool,
    state : Value? do
    def self.from_value(value : Value) : Disposition
      described = value.described? || raise DecodeError.new("expected disposition")
      raise DecodeError.new("expected disposition") unless described.descriptor_code? == Descriptor::DISPOSITION
      fields = described.value.list? || raise DecodeError.new("disposition fields must be list")
      role_bool = fields[0]?.try(&.bool?) || raise DecodeError.new("disposition missing role")
      role = role_bool ? Role::Receiver : Role::Sender
      first = fields[1]?.try(&.uint?).try(&.to_u32) || raise DecodeError.new("disposition missing first")
      last = fields[2]?.try(&.uint?).try(&.to_u32)
      settled = fields[3]?.try(&.bool?) || false
      state = fields[4]?
      new(role, first, last, settled, state)
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

  def self.outcome_value(outcome : Outcome, error : ErrorInfo? = nil) : Value
    case outcome
    in .accepted?
      Value.described(Value.ulong(Descriptor::ACCEPTED), Value.list(Array(Value).new))
    in .released?
      Value.described(Value.ulong(Descriptor::RELEASED), Value.list(Array(Value).new))
    in .rejected?
      fields = Array(Value).new(1)
      fields << (error.try(&.to_value) || Value.null)
      Value.described(Value.ulong(Descriptor::REJECTED), Value.list(fields))
    in .modified?
      Value.described(Value.ulong(Descriptor::MODIFIED), Value.list(Array(Value).new))
    end
  end

  def self.outcome_from_value(value : Value?) : Outcome?
    return unless described = value.try &.described?
    case described.descriptor_code?
    when Descriptor::ACCEPTED then Outcome::Accepted
    when Descriptor::RELEASED then Outcome::Released
    when Descriptor::REJECTED then Outcome::Rejected
    when Descriptor::MODIFIED then Outcome::Modified
    end
  end
end
