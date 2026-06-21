require "./performatives"

module LavinMQ
  module AMQP10
    # The AMQP 1.0 performatives, message terminus descriptors, delivery states
    # and SASL frames LavinMQ needs. Each decodes from a `Described` (read
    # generically off the wire) and encodes itself with correct field widths via
    # `to_io`, so it can be handed to `FrameWriter.write`.

    # amqp:error (transport §2.8.15) — carried by close/end/detach/rejected.
    struct ErrorPerformative
      getter condition : String
      getter description : String?

      def initialize(@condition : String, @description : String? = nil)
      end

      def self.decode(d : Described) : ErrorPerformative
        f = FieldReader.from(d)
        ErrorPerformative.new(f.symbol? || "amqp:internal-error", f.string?)
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.symbol @condition
        fl.string @description
        fl.value nil # info map
        Codec.write_described_list(io, Descriptor::ERROR, fl.fields)
      end
    end

    # open (transport §2.7.1)
    struct Open
      getter container_id : String
      getter hostname : String?
      getter max_frame_size : UInt32
      getter channel_max : UInt16
      getter idle_timeout : UInt32?

      def initialize(@container_id : String, @hostname : String? = nil,
                     @max_frame_size : UInt32 = UInt32::MAX, @channel_max : UInt16 = UInt16::MAX,
                     @idle_timeout : UInt32? = nil)
      end

      def self.decode(d : Described) : Open
        f = FieldReader.from(d)
        Open.new(
          container_id: f.string? || "",
          hostname: f.string?,
          max_frame_size: f.u32(UInt32::MAX),
          channel_max: f.u16(UInt16::MAX),
          idle_timeout: f.u32?,
        )
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.string @container_id
        fl.string @hostname
        fl.uint @max_frame_size
        fl.ushort @channel_max
        fl.uint @idle_timeout
        Codec.write_described_list(io, Descriptor::OPEN, fl.fields)
      end
    end

    # begin (transport §2.7.2)
    struct Begin
      getter remote_channel : UInt16?
      getter next_outgoing_id : UInt32
      getter incoming_window : UInt32
      getter outgoing_window : UInt32
      getter handle_max : UInt32

      def initialize(@next_outgoing_id : UInt32, @incoming_window : UInt32,
                     @outgoing_window : UInt32, @remote_channel : UInt16? = nil,
                     @handle_max : UInt32 = UInt32::MAX)
      end

      def self.decode(d : Described) : Begin
        f = FieldReader.from(d)
        Begin.new(
          remote_channel: f.u16?,
          next_outgoing_id: f.u32(0_u32),
          incoming_window: f.u32(0_u32),
          outgoing_window: f.u32(0_u32),
          handle_max: f.u32(UInt32::MAX),
        )
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.ushort @remote_channel
        fl.uint @next_outgoing_id
        fl.uint @incoming_window
        fl.uint @outgoing_window
        fl.uint @handle_max
        Codec.write_described_list(io, Descriptor::BEGIN, fl.fields)
      end
    end

    # source terminus (messaging §3.5.3)
    struct Source
      getter address : String?
      getter durable : UInt32
      getter expiry_policy : String?
      getter? dynamic : Bool

      def initialize(@address : String? = nil, @dynamic : Bool = false,
                     @durable : UInt32 = 0_u32, @expiry_policy : String? = nil)
      end

      def self.decode(d : Described) : Source
        f = FieldReader.from(d)
        addr = f.string?
        durable = f.u32(0_u32)
        expiry = f.symbol?
        f.u32? # timeout
        dynamic = f.bool(false)
        Source.new(address: addr, dynamic: dynamic, durable: durable, expiry_policy: expiry)
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.string @address
        fl.uint @durable
        fl.symbol @expiry_policy
        fl.uint nil # timeout
        fl.bool @dynamic
        Codec.write_described_list(io, Descriptor::SOURCE, fl.fields)
      end
    end

    # target terminus (messaging §3.5.4)
    struct Target
      getter address : String?
      getter durable : UInt32
      getter expiry_policy : String?
      getter? dynamic : Bool

      def initialize(@address : String? = nil, @dynamic : Bool = false,
                     @durable : UInt32 = 0_u32, @expiry_policy : String? = nil)
      end

      def self.decode(d : Described) : Target
        f = FieldReader.from(d)
        addr = f.string?
        durable = f.u32(0_u32)
        expiry = f.symbol?
        f.u32? # timeout
        dynamic = f.bool(false)
        Target.new(address: addr, dynamic: dynamic, durable: durable, expiry_policy: expiry)
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.string @address
        fl.uint @durable
        fl.symbol @expiry_policy
        fl.uint nil # timeout
        fl.bool @dynamic
        Codec.write_described_list(io, Descriptor::TARGET, fl.fields)
      end
    end

    # attach (transport §2.7.3). role: false=sender, true=receiver (peer's view).
    struct Attach
      ROLE_SENDER   = false
      ROLE_RECEIVER = true

      SND_UNSETTLED = 0_u8
      SND_SETTLED   = 1_u8
      SND_MIXED     = 2_u8
      RCV_FIRST     = 0_u8
      RCV_SECOND    = 1_u8

      getter name : String
      getter handle : UInt32
      getter? role : Bool
      getter snd_settle_mode : UInt8
      getter rcv_settle_mode : UInt8
      getter source : Source?
      getter target : Target?
      getter initial_delivery_count : UInt32?
      getter max_message_size : UInt64?

      def initialize(@name : String, @handle : UInt32, @role : Bool,
                     @source : Source? = nil, @target : Target? = nil,
                     @snd_settle_mode : UInt8 = SND_MIXED, @rcv_settle_mode : UInt8 = RCV_FIRST,
                     @initial_delivery_count : UInt32? = nil, @max_message_size : UInt64? = nil)
      end

      def self.decode(d : Described) : Attach
        f = FieldReader.from(d)
        name = f.string? || ""
        handle = f.u32(0_u32)
        role = f.bool(false)
        snd = f.u8(SND_MIXED)
        rcv = f.u8(RCV_FIRST)
        source_d = f.described?
        target_d = f.described?
        f.map?  # unsettled
        f.bool? # incomplete-unsettled
        idc = f.u32?
        max_size = f.u64?
        Attach.new(
          name: name, handle: handle, role: role,
          snd_settle_mode: snd, rcv_settle_mode: rcv,
          source: source_d.try { |sd| Source.decode(sd) },
          target: target_d.try { |td| Target.decode(td) },
          initial_delivery_count: idc, max_message_size: max_size,
        )
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.string @name
        fl.uint @handle
        fl.bool @role
        fl.ubyte @snd_settle_mode
        fl.ubyte @rcv_settle_mode
        fl.composite @source
        fl.composite @target
        fl.value nil  # unsettled
        fl.bool false # incomplete-unsettled
        fl.uint @initial_delivery_count
        fl.ulong @max_message_size
        Codec.write_described_list(io, Descriptor::ATTACH, fl.fields)
      end
    end

    # flow (transport §2.7.4)
    struct Flow
      getter next_incoming_id : UInt32?
      getter incoming_window : UInt32
      getter next_outgoing_id : UInt32
      getter outgoing_window : UInt32
      getter handle : UInt32?
      getter delivery_count : UInt32?
      getter link_credit : UInt32?
      getter available : UInt32?
      getter? drain : Bool
      getter? echo : Bool

      def initialize(@incoming_window : UInt32, @next_outgoing_id : UInt32,
                     @outgoing_window : UInt32, @next_incoming_id : UInt32? = nil,
                     @handle : UInt32? = nil, @delivery_count : UInt32? = nil,
                     @link_credit : UInt32? = nil, @available : UInt32? = nil,
                     @drain : Bool = false, @echo : Bool = false)
      end

      def self.decode(d : Described) : Flow
        f = FieldReader.from(d)
        Flow.new(
          next_incoming_id: f.u32?,
          incoming_window: f.u32(0_u32),
          next_outgoing_id: f.u32(0_u32),
          outgoing_window: f.u32(0_u32),
          handle: f.u32?,
          delivery_count: f.u32?,
          link_credit: f.u32?,
          available: f.u32?,
          drain: f.bool(false),
          echo: f.bool(false),
        )
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.uint @next_incoming_id
        fl.uint @incoming_window
        fl.uint @next_outgoing_id
        fl.uint @outgoing_window
        fl.uint @handle
        fl.uint @delivery_count
        fl.uint @link_credit
        fl.uint @available
        fl.bool @drain
        fl.bool @echo
        Codec.write_described_list(io, Descriptor::FLOW, fl.fields)
      end
    end

    # transfer (transport §2.7.5). The message payload follows the performative
    # in the frame body, handled separately by the frame layer.
    struct Transfer
      getter handle : UInt32
      getter delivery_id : UInt32?
      getter delivery_tag : Bytes?
      getter message_format : UInt32
      getter settled : Bool?
      getter? more : Bool
      getter rcv_settle_mode : UInt8?
      getter state : Described?

      def initialize(@handle : UInt32, @delivery_id : UInt32? = nil,
                     @delivery_tag : Bytes? = nil, @message_format : UInt32 = 0_u32,
                     @settled : Bool? = nil, @more : Bool = false,
                     @rcv_settle_mode : UInt8? = nil, @state : Described? = nil)
      end

      def self.decode(d : Described) : Transfer
        f = FieldReader.from(d)
        Transfer.new(
          handle: f.u32(0_u32),
          delivery_id: f.u32?,
          delivery_tag: f.bytes?,
          message_format: f.u32(0_u32),
          settled: f.bool?,
          more: f.bool(false),
          rcv_settle_mode: f.u8?,
          state: f.described?,
        )
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.uint @handle
        fl.uint @delivery_id
        fl.binary @delivery_tag
        fl.uint @message_format
        fl.bool @settled
        fl.bool @more
        fl.ubyte @rcv_settle_mode
        fl.value @state
        Codec.write_described_list(io, Descriptor::TRANSFER, fl.fields)
      end
    end

    # disposition (transport §2.7.6)
    struct Disposition
      getter? role : Bool
      getter first : UInt32
      getter last : UInt32?
      getter? settled : Bool
      getter state : Described?

      def initialize(@role : Bool, @first : UInt32, @last : UInt32? = nil,
                     @settled : Bool = false, @state : Described? = nil)
      end

      def self.decode(d : Described) : Disposition
        f = FieldReader.from(d)
        Disposition.new(
          role: f.bool(false),
          first: f.u32(0_u32),
          last: f.u32?,
          settled: f.bool(false),
          state: f.described?,
        )
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.bool @role
        fl.uint @first
        fl.uint @last
        fl.bool @settled
        fl.value @state
        Codec.write_described_list(io, Descriptor::DISPOSITION, fl.fields)
      end
    end

    # detach (transport §2.7.7)
    struct Detach
      getter handle : UInt32
      getter? closed : Bool
      getter error : ErrorPerformative?

      def initialize(@handle : UInt32, @closed : Bool = true, @error : ErrorPerformative? = nil)
      end

      def self.decode(d : Described) : Detach
        f = FieldReader.from(d)
        handle = f.u32(0_u32)
        closed = f.bool(false)
        err_d = f.described?
        Detach.new(handle, closed, err_d.try { |e| ErrorPerformative.decode(e) })
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.uint @handle
        fl.bool @closed
        fl.composite @error
        Codec.write_described_list(io, Descriptor::DETACH, fl.fields)
      end
    end

    # end (transport §2.7.8)
    struct End
      getter error : ErrorPerformative?

      def initialize(@error : ErrorPerformative? = nil)
      end

      def self.decode(d : Described) : End
        f = FieldReader.from(d)
        End.new(f.described?.try { |e| ErrorPerformative.decode(e) })
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.composite @error
        Codec.write_described_list(io, Descriptor::END_, fl.fields)
      end
    end

    # close (transport §2.7.9)
    struct Close
      getter error : ErrorPerformative?

      def initialize(@error : ErrorPerformative? = nil)
      end

      def self.decode(d : Described) : Close
        f = FieldReader.from(d)
        Close.new(f.described?.try { |e| ErrorPerformative.decode(e) })
      end

      def to_io(io : IO) : Nil
        fl = FieldList.new
        fl.composite @error
        Codec.write_described_list(io, Descriptor::CLOSE, fl.fields)
      end
    end
  end
end
