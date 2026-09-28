require "./spec_helper"

private class AMQP10SpecClient
  getter io, reader
  # The Flow the server sent right after the last attach_sender.
  getter attach_flow : LavinMQ::AMQP10::Flow?
  # The error condition of the detach read by the last attach_*_detached.
  getter last_detach_condition : String?

  def initialize(port : Int32, username = "guest", password = "guest", hostname : String? = nil,
                 frame_max = LavinMQ::Config.instance.frame_max, split_transport_header = false,
                 idle_timeout : UInt32? = nil, expect_open = true,
                 incoming_window : UInt32 = LavinMQ::AMQP10::DEFAULT_WINDOW, mechanism : String? = "PLAIN")
    @io = TCPSocket.new("localhost", port)
    @io.read_timeout = 5.seconds
    @reader = LavinMQ::AMQP10::FrameReader.new(@io, LavinMQ::Config.instance.frame_max)
    # A nil mechanism skips the SASL layer.
    sasl_handshake(username, password, mechanism) if mechanism
    send_transport_header(split_transport_header)
    send_open(hostname, frame_max, idle_timeout)
    # Callers that expect the server to refuse the connection read the reply themselves.
    return unless expect_open
    read_performative_code.should eq LavinMQ::AMQP10::Descriptor::OPEN
    begin_session(incoming_window)
    begin_frame = LavinMQ::AMQP10::Begin.from_value(read_value)
    begin_frame.remote_channel.should eq 0_u16
  end

  def self.authenticate(port : Int32, username, password, authzid = "") : UInt8
    io = TCPSocket.new("localhost", port)
    io.read_timeout = 5.seconds
    reader = LavinMQ::AMQP10::FrameReader.new(io, LavinMQ::Config.instance.frame_max)
    io.write LavinMQ::AMQP10::SASL_HEADER
    io.flush
    header = Bytes.new(8)
    io.read_fully(header)
    header.should eq LavinMQ::AMQP10::SASL_HEADER
    reader.read
    response = "#{authzid}\0#{username}\0#{password}"
    fields = [LavinMQ::AMQP10::Value.symbol("PLAIN"), LavinMQ::AMQP10::Value.binary(response.to_slice)]
    LavinMQ::AMQP10::FrameWriter.write_performative(io, 0_u16, LavinMQ::AMQP10::SASL_FRAME_TYPE,
      LavinMQ::AMQP10::Descriptor::SASL_INIT, fields)
    frame = reader.read
    value = LavinMQ::AMQP10::Codec.decode(frame.body_reader)
    fields = value.described?.not_nil!.value.list?.not_nil!
    fields[0].uint?.not_nil!.to_u8
  ensure
    io.try &.close
  end

  def close
    @io.close
  end

  def expect_no_frame(timeout = 50.milliseconds) : Nil
    @io.read_timeout = timeout
    expect_raises(IO::TimeoutError) do
      @reader.read
    end
  ensure
    @io.read_timeout = 5.seconds
  end

  def attach_sender(address : String?, handle = 0_u32, name = "sender", dynamic = false,
                    snd_settle_mode : UInt8? = nil, rcv_settle_mode : UInt8? = nil,
                    initial_delivery_count : UInt32? = nil) : LavinMQ::AMQP10::Attach
    target = LavinMQ::AMQP10::Target.new(address, dynamic: dynamic).to_value
    fields = attach_fields(name, handle, role_receiver: false,
      source: LavinMQ::AMQP10::Value.null, target: target,
      snd_settle_mode: snd_settle_mode, rcv_settle_mode: rcv_settle_mode,
      initial_delivery_count: initial_delivery_count)
    send_performative(LavinMQ::AMQP10::Descriptor::ATTACH, fields)
    attach = LavinMQ::AMQP10::Attach.from_value(read_value)
    flow = LavinMQ::AMQP10::Flow.from_value(read_value)
    flow.next_incoming_id.should_not be_nil
    flow.incoming_window.should_not be_nil
    flow.next_outgoing_id.should_not be_nil
    flow.outgoing_window.should_not be_nil
    flow.link_credit.should eq LavinMQ::AMQP10::ReceiverLink::LINK_CREDIT
    @attach_flow = flow
    attach
  end

  def attach_sender_detached(address : String?, handle = 0_u32, name = "sender",
                             dynamic = false, durable = 0_u32,
                             dynamic_node_properties : LavinMQ::AMQP10::Value? = nil) : LavinMQ::AMQP10::Detach
    target = LavinMQ::AMQP10::Target.new(address, durable: durable, dynamic: dynamic,
      dynamic_node_properties: dynamic_node_properties).to_value
    fields = attach_fields(name, handle, role_receiver: false,
      source: LavinMQ::AMQP10::Value.null, target: target)
    send_performative(LavinMQ::AMQP10::Descriptor::ATTACH, fields)
    attach = LavinMQ::AMQP10::Attach.from_value(read_value)
    attach.name.should eq name
    attach.role.should eq LavinMQ::AMQP10::Role::Receiver
    attach.source.should be_nil
    attach.target.should be_nil
    detach_value = read_value
    @last_detach_condition = error_condition(detach_value)
    detach = LavinMQ::AMQP10::Detach.from_value(detach_value)
    detach.handle.should eq attach.handle
    detach
  end

  def attach_receiver(address : String?, handle = 0_u32, name = "receiver", dynamic = false,
                      snd_settle_mode : UInt8? = nil, rcv_settle_mode : UInt8? = nil) : LavinMQ::AMQP10::Attach
    source = LavinMQ::AMQP10::Source.new(address, dynamic: dynamic).to_value
    fields = attach_fields(name, handle, role_receiver: true,
      source: source, target: LavinMQ::AMQP10::Value.null,
      snd_settle_mode: snd_settle_mode, rcv_settle_mode: rcv_settle_mode)
    send_performative(LavinMQ::AMQP10::Descriptor::ATTACH, fields)
    frame = read_value
    LavinMQ::AMQP10::Attach.from_value(frame)
  end

  def attach_receiver_detached(address : String?, handle = 0_u32, name = "receiver",
                               dynamic = false, durable = 0_u32,
                               dynamic_node_properties : LavinMQ::AMQP10::Value? = nil) : LavinMQ::AMQP10::Detach
    source = LavinMQ::AMQP10::Source.new(address, durable: durable, dynamic: dynamic,
      dynamic_node_properties: dynamic_node_properties).to_value
    fields = attach_fields(name, handle, role_receiver: true,
      source: source, target: LavinMQ::AMQP10::Value.null)
    send_performative(LavinMQ::AMQP10::Descriptor::ATTACH, fields)
    attach = LavinMQ::AMQP10::Attach.from_value(read_value)
    attach.name.should eq name
    attach.role.should eq LavinMQ::AMQP10::Role::Sender
    attach.source.should be_nil
    attach.target.should be_nil
    attach.initial_delivery_count.should eq 0_u32
    detach_value = read_value
    @last_detach_condition = error_condition(detach_value)
    detach = LavinMQ::AMQP10::Detach.from_value(detach_value)
    detach.handle.should eq attach.handle
    detach
  end

  def flow(handle = 0_u32, credit = 1_u32, delivery_count = 0_u32, drain = false, echo = false) : Nil
    fields = Array(LavinMQ::AMQP10::Value).new(10)
    4.times { fields << LavinMQ::AMQP10::Value.null }
    fields << LavinMQ::AMQP10::Value.uint(handle)
    fields << LavinMQ::AMQP10::Value.uint(delivery_count)
    fields << LavinMQ::AMQP10::Value.uint(credit)
    if drain || echo
      fields << LavinMQ::AMQP10::Value.null # available
      fields << LavinMQ::AMQP10::Value.bool(drain)
      fields << LavinMQ::AMQP10::Value.bool(echo)
    end
    send_performative(LavinMQ::AMQP10::Descriptor::FLOW, fields)
  end

  # Session-level flow (no handle): tells the server how many more transfers
  # our incoming-window accepts, counted from next_incoming_id.
  def session_flow(next_incoming_id : UInt32, incoming_window : UInt32) : Nil
    fields = [
      LavinMQ::AMQP10::Value.uint(next_incoming_id),
      LavinMQ::AMQP10::Value.uint(incoming_window),
      LavinMQ::AMQP10::Value.uint(0_u32), # next-outgoing-id
      LavinMQ::AMQP10::Value.uint(LavinMQ::AMQP10::DEFAULT_WINDOW),
    ]
    send_performative(LavinMQ::AMQP10::Descriptor::FLOW, fields)
  end

  def read_flow : LavinMQ::AMQP10::Flow
    LavinMQ::AMQP10::Flow.from_value(read_value)
  end

  def publish(handle : UInt32, delivery_id : UInt32, body : String, to : String? = nil) : LavinMQ::AMQP10::Outcome
    write_publish(handle, delivery_id, body, to)

    frame = @reader.read
    disposition = LavinMQ::AMQP10::TransferCodec.read_disposition(frame.body_reader)
    disposition.outcome.not_nil!
  end

  def publish_reading_flows(handle : UInt32, delivery_id : UInt32, body : String, to : String? = nil)
    write_publish(handle, delivery_id, body, to)
    flows = [] of LavinMQ::AMQP10::Flow

    loop do
      frame = @reader.read
      code = LavinMQ::AMQP10::Codec.read_descriptor_code(frame.body_reader)
      case code
      when LavinMQ::AMQP10::Descriptor::FLOW
        flows << LavinMQ::AMQP10::Flow.from_value(LavinMQ::AMQP10::Codec.decode(frame.body_reader))
      when LavinMQ::AMQP10::Descriptor::DISPOSITION
        disposition = LavinMQ::AMQP10::TransferCodec.read_disposition(frame.body_reader)
        return {flows, disposition.outcome.not_nil!}
      else
        fail "unexpected AMQP 1.0 performative #{code}"
      end
    end
  end

  def publish_fragmented(handle : UInt32, delivery_id : UInt32, body : String) : LavinMQ::AMQP10::Outcome
    message = IO::Memory.new
    message.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(message, LavinMQ::AMQP10::Descriptor::DATA)
    LavinMQ::AMQP10::Codec.write_binary(message, body.to_slice)
    message_bytes = message.to_slice
    split = message_bytes.bytesize // 2
    tag = delivery_id.to_s.to_slice

    first = IO::Memory.new
    LavinMQ::AMQP10::TransferCodec.write_transfer_performative(first, handle, delivery_id, tag, true, false)
    first.write message_bytes[0, split]
    write_amqp_frame(first.to_slice)

    second = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_described_list(second, LavinMQ::AMQP10::Descriptor::TRANSFER, [
      LavinMQ::AMQP10::Value.uint(handle),
    ])
    second.write message_bytes[split, message_bytes.bytesize - split]
    write_amqp_frame(second.to_slice)

    frame = @reader.read
    disposition = LavinMQ::AMQP10::TransferCodec.read_disposition(frame.body_reader)
    disposition.outcome.not_nil!
  end

  # A single-frame transfer that omits the mandatory delivery-id.
  def write_transfer_without_delivery_id(handle : UInt32, body : String) : Nil
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::TRANSFER, [
      LavinMQ::AMQP10::Value.uint(handle),
    ])
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::DATA)
    LavinMQ::AMQP10::Codec.write_binary(payload, body.to_slice)
    write_amqp_frame(payload.to_slice)
  end

  def publish_oversized_fragment(handle : UInt32, delivery_id : UInt32, payload_size : Int32) : LavinMQ::AMQP10::Outcome
    payload = IO::Memory.new
    tag = delivery_id.to_s.to_slice
    LavinMQ::AMQP10::TransferCodec.write_transfer_performative(payload, handle, delivery_id, tag, true, false)
    payload.write Bytes.new(payload_size, 'x'.ord.to_u8)
    write_amqp_frame(payload.to_slice)

    frame = @reader.read
    disposition = LavinMQ::AMQP10::TransferCodec.read_disposition(frame.body_reader)
    disposition.outcome.not_nil!
  end

  def consume_one(outcome = LavinMQ::AMQP10::Outcome::Accepted) : String
    incoming = consume_one_message(outcome)
    String.new(incoming.body)
  end

  def consume_one_message(outcome = LavinMQ::AMQP10::Outcome::Accepted) : LavinMQ::AMQP10::MessageCodec::Incoming
    consume_one_delivery(outcome)[1]
  end

  def consume_one_delivery(outcome = LavinMQ::AMQP10::Outcome::Accepted)
    transfer, incoming = read_delivery
    settle(transfer.delivery_id.not_nil!, outcome)
    {transfer, incoming}
  end

  # The message sections of the next (single-frame) delivery.
  def read_delivery_sections : Tuple(LavinMQ::AMQP10::TransferCodec::TransferView, Array(Tuple(UInt64, Bytes)))
    reader = @reader.read.body_reader
    transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
    {transfer, message_sections(reader.peek.dup)}
  end

  def read_delivery
    frame = @reader.read
    reader = frame.body_reader
    transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
    incoming = LavinMQ::AMQP10::MessageCodec.decode(reader)
    {transfer, incoming}
  end

  def settle(delivery_id : UInt32, outcome = LavinMQ::AMQP10::Outcome::Accepted, settled = true,
             role = LavinMQ::AMQP10::Role::Receiver) : Nil
    LavinMQ::AMQP10::TransferCodec.write_disposition(@io, 0_u16,
      delivery_id, outcome, settled, role)
  end

  def read_disposition : LavinMQ::AMQP10::TransferCodec::DispositionView
    LavinMQ::AMQP10::TransferCodec.read_disposition(@reader.read.body_reader)
  end

  def read_detach : LavinMQ::AMQP10::Detach
    LavinMQ::AMQP10::Detach.from_value(read_value)
  end

  def consume_one_fragmented(outcome = LavinMQ::AMQP10::Outcome::Accepted, max_frame_size : UInt32? = nil) : String
    payload = IO::Memory.new
    delivery_id = nil
    loop do
      frame = @reader.read
      if max = max_frame_size
        (frame.body.bytesize + 8).should be <= max.to_i
      end
      reader = frame.body_reader
      transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
      delivery_id ||= transfer.delivery_id
      payload.write reader.peek
      break unless transfer.more
    end
    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))
    LavinMQ::AMQP10::TransferCodec.write_disposition(@io, 0_u16, delivery_id.not_nil!, outcome)
    String.new(incoming.body)
  end

  # A detach the server should not answer, e.g. the reply to its own detach.
  def send_detach(handle : UInt32) : Nil
    fields = [LavinMQ::AMQP10::Value.uint(handle), LavinMQ::AMQP10::Value.bool(true)]
    send_performative(LavinMQ::AMQP10::Descriptor::DETACH, fields)
  end

  def end_session(channel : UInt16 = 0_u16) : Nil
    send_performative(LavinMQ::AMQP10::Descriptor::END, Array(LavinMQ::AMQP10::Value).new, channel)
  end

  def detach(handle = 0_u32) : Nil
    fields = [LavinMQ::AMQP10::Value.uint(handle), LavinMQ::AMQP10::Value.bool(true)]
    send_performative(LavinMQ::AMQP10::Descriptor::DETACH, fields)
    read_performative_code.should eq LavinMQ::AMQP10::Descriptor::DETACH
  end

  private def sasl_handshake(username, password, mechanism)
    @io.write LavinMQ::AMQP10::SASL_HEADER
    @io.flush
    header = Bytes.new(8)
    @io.read_fully(header)
    header.should eq LavinMQ::AMQP10::SASL_HEADER
    @reader.read # sasl-mechanisms
    response = mechanism == "PLAIN" ? "\0#{username}\0#{password}" : ""
    fields = [LavinMQ::AMQP10::Value.symbol(mechanism), LavinMQ::AMQP10::Value.binary(response.to_slice)]
    LavinMQ::AMQP10::FrameWriter.write_performative(@io, 0_u16, LavinMQ::AMQP10::SASL_FRAME_TYPE,
      LavinMQ::AMQP10::Descriptor::SASL_INIT, fields)
    frame = @reader.read
    value = LavinMQ::AMQP10::Codec.decode(frame.body_reader)
    fields = value.described?.not_nil!.value.list?.not_nil!
    fields[0].uint?.not_nil!.should eq 0
  end

  private def send_transport_header(split_transport_header : Bool) : Nil
    if split_transport_header
      @io.write LavinMQ::AMQP10::PROTOCOL_HEADER[0, 4]
      @io.flush
      sleep 20.milliseconds
      @io.write LavinMQ::AMQP10::PROTOCOL_HEADER[4, 4]
    else
      @io.write LavinMQ::AMQP10::PROTOCOL_HEADER
    end
    @io.flush
    header = Bytes.new(8)
    @io.read_fully(header)
    header.should eq LavinMQ::AMQP10::PROTOCOL_HEADER
  end

  private def send_open(hostname, frame_max, idle_timeout : UInt32? = nil)
    fields = [LavinMQ::AMQP10::Value.string("spec-client")]
    fields << (hostname ? LavinMQ::AMQP10::Value.string(hostname) : LavinMQ::AMQP10::Value.null)
    fields << LavinMQ::AMQP10::Value.uint(frame_max)
    if idle_timeout
      fields << LavinMQ::AMQP10::Value.null # channel-max
      fields << LavinMQ::AMQP10::Value.uint(idle_timeout)
    end
    send_performative(LavinMQ::AMQP10::Descriptor::OPEN, fields)
  end

  def begin_session(incoming_window : UInt32 = LavinMQ::AMQP10::DEFAULT_WINDOW, channel : UInt16 = 0_u16) : Nil
    fields = [
      LavinMQ::AMQP10::Value.null,
      LavinMQ::AMQP10::Value.uint(0_u32),
      LavinMQ::AMQP10::Value.uint(incoming_window),
      LavinMQ::AMQP10::Value.uint(LavinMQ::AMQP10::DEFAULT_WINDOW),
    ]
    send_performative(LavinMQ::AMQP10::Descriptor::BEGIN, fields, channel)
  end

  private def attach_fields(name, handle, role_receiver, source, target,
                            snd_settle_mode : UInt8? = nil, rcv_settle_mode : UInt8? = nil,
                            initial_delivery_count : UInt32? = nil)
    fields = Array(LavinMQ::AMQP10::Value).new(10)
    fields << LavinMQ::AMQP10::Value.string(name)
    fields << LavinMQ::AMQP10::Value.uint(handle)
    fields << LavinMQ::AMQP10::Value.bool(role_receiver)
    fields << (snd_settle_mode ? LavinMQ::AMQP10::Value.ubyte(snd_settle_mode) : LavinMQ::AMQP10::Value.null)
    fields << (rcv_settle_mode ? LavinMQ::AMQP10::Value.ubyte(rcv_settle_mode) : LavinMQ::AMQP10::Value.null)
    fields << source
    fields << target
    if initial_delivery_count
      fields << LavinMQ::AMQP10::Value.null # unsettled
      fields << LavinMQ::AMQP10::Value.null # incomplete-unsettled
      fields << LavinMQ::AMQP10::Value.uint(initial_delivery_count)
    end
    fields
  end

  private def send_performative(code, fields, channel : UInt16 = 0_u16)
    LavinMQ::AMQP10::FrameWriter.write_performative(@io, channel,
      LavinMQ::AMQP10::AMQP_FRAME_TYPE, code, fields)
  end

  # Publishes an already encoded message and returns the outcome.
  def publish_raw(handle : UInt32, delivery_id : UInt32, message : Bytes) : LavinMQ::AMQP10::Outcome
    payload = IO::Memory.new
    LavinMQ::AMQP10::TransferCodec.write_transfer_performative(payload, handle, delivery_id, delivery_id.to_s.to_slice, false, false)
    payload.write message
    write_amqp_frame(payload.to_slice)
    LavinMQ::AMQP10::TransferCodec.read_disposition(@reader.read.body_reader).outcome.not_nil!
  end

  # Settles a delivery with a modified outcome carrying message-annotations.
  def settle_modified(delivery_id : UInt32, annotations : LavinMQ::AMQP10::Value, delivery_failed = true) : Nil
    modified = LavinMQ::AMQP10::Value.described(LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::MODIFIED),
      LavinMQ::AMQP10::Value.list([LavinMQ::AMQP10::Value.bool(delivery_failed), LavinMQ::AMQP10::Value.bool(false), annotations]))
    fields = [LavinMQ::AMQP10::Value.bool(true), LavinMQ::AMQP10::Value.uint(delivery_id), LavinMQ::AMQP10::Value.null,
              LavinMQ::AMQP10::Value.bool(true), modified]
    send_performative(LavinMQ::AMQP10::Descriptor::DISPOSITION, fields)
  end

  # Pre-settled transfer: the server publishes it without replying with a disposition.
  def publish_settled(handle : UInt32, delivery_id : UInt32, body : String) : Nil
    write_publish(handle, delivery_id, body, settled: true)
  end

  def write_publish(handle : UInt32, delivery_id : UInt32, body : String, to : String? = nil,
                    settled = false) : Nil
    payload = IO::Memory.new
    tag = delivery_id.to_s.to_slice
    LavinMQ::AMQP10::TransferCodec.write_transfer_performative(payload, handle, delivery_id, tag, false, settled)
    if to
      fields = [LavinMQ::AMQP10::Value.null, LavinMQ::AMQP10::Value.null, LavinMQ::AMQP10::Value.string(to)]
      LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::PROPERTIES, fields)
    end
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::DATA)
    LavinMQ::AMQP10::Codec.write_binary(payload, body.to_slice)
    LavinMQ::AMQP10::FrameWriter.write_frame_header(@io, (8 + payload.size).to_u32,
      LavinMQ::AMQP10::AMQP_FRAME_TYPE, 0_u16)
    @io.write payload.to_slice
    @io.flush
  end

  private def write_amqp_frame(payload : Bytes) : Nil
    LavinMQ::AMQP10::FrameWriter.write_frame_header(@io, (8 + payload.bytesize).to_u32,
      LavinMQ::AMQP10::AMQP_FRAME_TYPE, 0_u16)
    @io.write payload
    @io.flush
  end

  # A frame of the given total size whose body is junk.
  def send_junk_frame(size : Int32) : Nil
    write_amqp_frame(Bytes.new(size - 8, 0x40_u8))
  end

  def send_empty_frame : Nil
    LavinMQ::AMQP10::FrameWriter.write_frame_header(@io, 8_u32, LavinMQ::AMQP10::AMQP_FRAME_TYPE, 0_u16)
    @io.flush
  end

  def read_performative_code
    read_value.described?.not_nil!.descriptor_code?
  end

  def read_value
    LavinMQ::AMQP10::Codec.decode(@reader.read.body_reader)
  end

  # The error condition carried by a performative whose last field is an error, if any.
  def error_condition(performative : LavinMQ::AMQP10::Value) : String?
    fields = performative.described?.try(&.value.list?) || return
    error = fields.last?.try(&.described?) || return
    error.value.list?.try(&.first?).try(&.symbol?)
  end

  # The error list carried by a Close or Detach performative: [condition, description]
  def error_fields(performative : LavinMQ::AMQP10::Value) : Array(LavinMQ::AMQP10::Value)
    error = performative.described?.not_nil!.value.list?.not_nil!.last
    error.described?.not_nil!.value.list?.not_nil!
  end
end

private class AMQP10PrioritySpecConsumer < LavinMQ::Client::Channel::Consumer
  getter tag = "amqp10-priority-spec"
  getter priority = 5
  getter? exclusive = false
  getter? no_ack = false
  getter queue
  getter has_capacity = BoolChannel.new(true)
  getter? closed = false

  def initialize(@queue : LavinMQ::AMQP::Queue)
  end

  def accepts? : Bool
    !@closed
  end

  def close
    return if @closed
    @closed = true
    @has_capacity.close
    @queue.rm_consumer(self)
  end

  def cancel
    close
  end

  def flow(active : Bool)
  end

  def ack(sp)
  end

  def reject(sp, requeue = false)
  end

  def deliver(msg, sp, redelivered = false, recover = false)
  end

  def unacked
    0
  end

  def prefetch_count
    0_u16
  end

  def prefetch_count=(value)
  end

  def unacked_messages
    [] of UnackedMessage
  end

  def details_tuple
    {consumer_tag: @tag}
  end
end

# Encodes `msg` as a single-frame delivery and returns the message sections
# after the transfer performative, as descriptor code => section bytes.
private def delivered_sections(msg : LavinMQ::BytesMessage, redelivered = false) : Array(Tuple(UInt64, Bytes))
  io = IO::Memory.new
  LavinMQ::AMQP10::MessageCodec.write_transfer(io, 0_u16, 0_u32, 7_u32, "tag".to_slice, msg, redelivered: redelivered)
  bytes = io.to_slice
  frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[0, 4])
  reader = IO::Memory.new(bytes[8, frame_size.to_i - 8])
  LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
  message_sections(reader.peek)
end

# Hands an AMQP 1.0 connection factory a client that sent the AMQP protocol
# header, skipping SASL, from connection_info. Returns the protocol header
# the server answers with; a SASL header is followed by a disconnect.
private def factory_without_sasl(s, connection_info) : Bytes
  client, server = UNIXSocket.pair
  client.read_timeout = 5.seconds
  factory = LavinMQ::AMQP10::ConnectionFactory.new(s.authenticator, s.vhosts)
  spawn { factory.start(server, connection_info, sasl: false) }
  header = Bytes.new(8)
  client.read_fully(header)
  if header == LavinMQ::AMQP10::SASL_HEADER
    eof = uninitialized UInt8[1]
    client.read(eof.to_slice).should eq 0
  end
  header
ensure
  client.try &.close
  server.try &.close
end

# Runs the SASL exchange against an AMQP 1.0 connection factory, as a client
# connecting from connection_info. Returns the advertised mechanisms and the
# sasl-outcome code.
private def factory_sasl(s, connection_info, mechanism, response = Bytes.empty) : Tuple(Array(String), UInt8)
  client, server = UNIXSocket.pair
  client.read_timeout = 5.seconds
  factory = LavinMQ::AMQP10::ConnectionFactory.new(s.authenticator, s.vhosts)
  spawn { factory.start(server, connection_info) }
  header = Bytes.new(8)
  client.read_fully(header)
  header.should eq LavinMQ::AMQP10::SASL_HEADER
  reader = LavinMQ::AMQP10::FrameReader.new(client, LavinMQ::Config.instance.frame_max)
  mechanisms = LavinMQ::AMQP10::Codec.decode(reader.read.body_reader).described?.not_nil!.value.list?.not_nil!
  offered = mechanisms[0].list?.not_nil!.map(&.symbol?.not_nil!)
  fields = [LavinMQ::AMQP10::Value.symbol(mechanism), LavinMQ::AMQP10::Value.binary(response)]
  LavinMQ::AMQP10::FrameWriter.write_performative(client, 0_u16, LavinMQ::AMQP10::SASL_FRAME_TYPE,
    LavinMQ::AMQP10::Descriptor::SASL_INIT, fields)
  outcome = LavinMQ::AMQP10::Codec.decode(reader.read.body_reader).described?.not_nil!.value.list?.not_nil!
  {offered, outcome[0].uint?.not_nil!.to_u8}
ensure
  client.try &.close
  server.try &.close
end

# Splits an encoded message into its sections, as descriptor code => section bytes.
private def message_sections(message : Bytes) : Array(Tuple(UInt64, Bytes))
  reader = IO::Memory.new(message)
  sections = [] of Tuple(UInt64, Bytes)
  while reader.pos < reader.bytesize
    start = reader.pos
    code = LavinMQ::AMQP10::Codec.read_descriptor_code(reader)
    LavinMQ::AMQP10::Codec.skip_value(reader)
    sections << {code, message[start, reader.pos - start]}
  end
  sections
end

private def annotations_map(pairs : Hash(String, LavinMQ::AMQP10::Value)) : LavinMQ::AMQP10::Value
  LavinMQ::AMQP10::Value.map(pairs.map { |k, v| {LavinMQ::AMQP10::Value.symbol(k), v} })
end

private def encoded(value : LavinMQ::AMQP10::Value) : Bytes
  io = IO::Memory.new
  LavinMQ::AMQP10::Codec.write_value(io, value)
  io.to_slice
end

# The entries of an encoded annotations map, by symbol key.
private def annotation_entries(map : Bytes) : Hash(String, LavinMQ::AMQP10::Value)
  LavinMQ::AMQP10::Codec.decode(IO::Memory.new(map)).map?.not_nil!.to_h { |k, v| {k.symbol?.not_nil!, v} }
end

# The fields of a list-bodied section such as header or properties.
private def section_fields(section : Bytes) : Array(LavinMQ::AMQP10::Value)
  LavinMQ::AMQP10::Codec.decode(IO::Memory.new(section)).described?.not_nil!.value.list?.not_nil!
end

# Decodes a published AMQP 1.0 message and stores it the way a queue would.
private def stored_message(payload : Bytes) : LavinMQ::BytesMessage
  incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload))
  LavinMQ::BytesMessage.new(1_i64, "", "rk", incoming.properties, incoming.body.bytesize.to_u64, incoming.body.dup)
end

private def amqp10_session(server : LavinMQ::Server) : LavinMQ::AMQP10::Session
  wait_for do
    found = nil.as(LavinMQ::AMQP10::Session?)
    server.connections.each do |conn|
      if client = conn.as?(LavinMQ::AMQP10::Client)
        if session = client.channel?(0_u16).as?(LavinMQ::AMQP10::Session)
          found = session
          break
        end
      end
    end
    found
  end
end

describe LavinMQ::AMQP10::MessageCodec do
  it "maps AMQP 1.0 header and application-properties to AMQP properties" do
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::HEADER, [
      LavinMQ::AMQP10::Value.bool(true),
      LavinMQ::AMQP10::Value.ubyte(7_u8),
    ])
    LavinMQ::AMQP10::Codec.write_value(payload,
      LavinMQ::AMQP10::Value.described(
        LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::APPLICATION_PROPERTIES),
        LavinMQ::AMQP10::Value.map([
          {LavinMQ::AMQP10::Value.string("app"), LavinMQ::AMQP10::Value.string("amqp10")},
          {LavinMQ::AMQP10::Value.symbol("enabled"), LavinMQ::AMQP10::Value.bool(true)},
          {LavinMQ::AMQP10::Value.string("tries"), LavinMQ::AMQP10::Value.uint(3_u32)},
          {LavinMQ::AMQP10::Value.string("ratio"), LavinMQ::AMQP10::Value.double(1.5_f64)},
        ])
      )
    )
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::DATA)
    LavinMQ::AMQP10::Codec.write_binary(payload, "body".to_slice)

    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))

    incoming.properties.delivery_mode.should eq 2_u8
    incoming.properties.priority.should eq 7_u8
    incoming.properties.headers.not_nil!["app"].should eq "amqp10"
    incoming.properties.headers.not_nil!["enabled"].should be_true
    incoming.properties.headers.not_nil!["tries"].should eq 3_u32
    incoming.properties.headers.not_nil!["ratio"].should eq 1.5_f64
    String.new(incoming.body).should eq "body"
  end

  it "uses string and binary amqp-value sections as the message body" do
    string_payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_value(string_payload,
      LavinMQ::AMQP10::Value.described(
        LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::AMQP_VALUE),
        LavinMQ::AMQP10::Value.string("value-body")
      )
    )
    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(string_payload.to_slice))
    String.new(incoming.body).should eq "value-body"

    binary_payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_value(binary_payload,
      LavinMQ::AMQP10::Value.described(
        LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::AMQP_VALUE),
        LavinMQ::AMQP10::Value.binary("binary-body".to_slice)
      )
    )
    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(binary_payload.to_slice))
    String.new(incoming.body).should eq "binary-body"
  end

  it "concatenates multiple data sections as the message body" do
    payload = IO::Memory.new
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::DATA)
    LavinMQ::AMQP10::Codec.write_binary(payload, "hello ".to_slice)
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::DATA)
    LavinMQ::AMQP10::Codec.write_binary(payload, "world".to_slice)

    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))

    String.new(incoming.body).should eq "hello world"
  end

  it "decodes UUID message-id and correlation-id properties" do
    message_id = Bytes[0x12_u8, 0x34_u8, 0x56_u8, 0x78_u8, 0x9a_u8, 0xbc_u8, 0xde_u8, 0xf0_u8,
      0x12_u8, 0x34_u8, 0x56_u8, 0x78_u8, 0x9a_u8, 0xbc_u8, 0xde_u8, 0xf0_u8]
    correlation_id = Bytes[0x0f_u8, 0xed_u8, 0xcb_u8, 0xa9_u8, 0x87_u8, 0x65_u8, 0x43_u8, 0x21_u8,
      0x0f_u8, 0xed_u8, 0xcb_u8, 0xa9_u8, 0x87_u8, 0x65_u8, 0x43_u8, 0x21_u8]
    payload = IO::Memory.new
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::PROPERTIES)
    payload.write_byte 0xc0_u8
    payload.write_byte 39_u8
    payload.write_byte 6_u8
    payload.write_byte 0x98_u8
    payload.write message_id
    4.times { payload.write_byte 0x40_u8 }
    payload.write_byte 0x98_u8
    payload.write correlation_id

    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))

    incoming.properties.message_id.should eq "12345678-9abc-def0-1234-56789abcdef0"
    incoming.properties.correlation_id.should eq "0fedcba9-8765-4321-0fed-cba987654321"
  end

  it "raises DecodeError for oversized message section lengths" do
    payload = IO::Memory.new
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::DATA)
    payload.write_byte 0xb0_u8
    LavinMQ::AMQP10::Codec.write_u32(payload, UInt32::MAX)

    expect_raises(LavinMQ::AMQP10::DecodeError) do
      LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))
    end
  end

  it "maps the header ttl field to the AMQP expiration" do
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::HEADER, [
      LavinMQ::AMQP10::Value.bool(true),
      LavinMQ::AMQP10::Value.ubyte(4_u8),
      LavinMQ::AMQP10::Value.uint(5000_u32),
    ])

    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))

    incoming.properties.delivery_mode.should eq 2_u8
    incoming.properties.priority.should eq 4_u8
    incoming.properties.expiration.should eq "5000"
  end

  it "decodes negative and compact-zero application-property integers" do
    payload = IO::Memory.new
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::APPLICATION_PROPERTIES)
    # map8 with 2 entries: {"neg" => smallint -1 (0x54 0xff)}, {"zero" => uint0 (0x43)}
    body = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_string(body, "neg")
    body.write_byte 0x54_u8
    body.write_byte 0xff_u8
    LavinMQ::AMQP10::Codec.write_string(body, "zero")
    body.write_byte 0x43_u8
    bytes = body.to_slice
    payload.write_byte 0xc1_u8
    payload.write_byte (bytes.bytesize + 1).to_u8
    payload.write_byte 4_u8
    payload.write bytes

    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))

    incoming.properties.headers.not_nil!["neg"].should eq -1
    incoming.properties.headers.not_nil!["zero"].should eq 0_u32
  end

  it "accepts symbolic section descriptors" do
    payload = IO::Memory.new
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_symbol(payload, "amqp:data:binary")
    LavinMQ::AMQP10::Codec.write_binary(payload, "sym-body".to_slice)

    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))

    String.new(incoming.body).should eq "sym-body"
  end

  it "preserves structured amqp-value bodies instead of dropping them" do
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_value(payload,
      LavinMQ::AMQP10::Value.described(
        LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::AMQP_VALUE),
        LavinMQ::AMQP10::Value.list([LavinMQ::AMQP10::Value.uint(1_u32), LavinMQ::AMQP10::Value.uint(2_u32)])
      )
    )

    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))

    incoming.body.empty?.should be_false
  end

  it "skips decimal128 values without raising" do
    data = Bytes.new(17)
    data[0] = 0x94_u8
    reader = IO::Memory.new(data)
    LavinMQ::AMQP10::Codec.skip_value(reader)
    reader.pos.should eq 17
  end

  it "clamps out-of-range creation-time timestamps to nil" do
    fields = Array(LavinMQ::AMQP10::Value).new(10)
    9.times { fields << LavinMQ::AMQP10::Value.null }
    fields << LavinMQ::AMQP10::Value.timestamp(Int64::MAX)
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::PROPERTIES, fields)

    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))

    incoming.properties.timestamp_raw.should be_nil
  end

  it "rejects string properties longer than 255 bytes" do
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::PROPERTIES, [
      LavinMQ::AMQP10::Value.string("x" * 300),
    ])

    expect_raises(LavinMQ::AMQP10::DecodeError) do
      LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))
    end
  end

  it "raises DecodeError for a truncated list8 header" do
    payload = IO::Memory.new
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::PROPERTIES)
    payload.write_byte 0xc0_u8 # list8
    payload.write_byte 250_u8  # declared size far exceeds the bytes that follow
    payload.write_byte 1_u8    # count
    payload.write_byte 0x40_u8 # message-id null

    expect_raises(LavinMQ::AMQP10::DecodeError) do
      LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))
    end
  end
end

describe "LavinMQ::AMQP10::MessageCodec.write_transfer" do
  it "delivers amqp-value bodies in the section type they were published with" do
    values = [
      LavinMQ::AMQP10::Value.string("text"),
      LavinMQ::AMQP10::Value.string("x" * 300),
      LavinMQ::AMQP10::Value.binary(Bytes[1, 2, 3]),
      LavinMQ::AMQP10::Value.list([LavinMQ::AMQP10::Value.uint(1_u32), LavinMQ::AMQP10::Value.string("a")]),
      LavinMQ::AMQP10::Value.null,
    ]
    values.each do |value|
      body = LavinMQ::AMQP10::Value.described(LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::AMQP_VALUE), value)
      payload = IO::Memory.new
      LavinMQ::AMQP10::Codec.write_value(payload, body)

      sections = delivered_sections(stored_message(payload.to_slice))

      # The internal body-type header is not an application property.
      sections.map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::AMQP_VALUE]
      sections[0][1].should eq payload.to_slice
    end
  end

  it "delivers message-id and correlation-id with the type they were published with" do
    uuid = UUID.random
    ids = {
      Bytes[0xa1, 3, 'a'.ord, 'b'.ord, 'c'.ord], # string
      Bytes[0x53, 7],                            # smallulong
      Bytes[0x80, 0, 0, 0, 0, 0, 0, 0x4e, 0x20], # ulong 20000
      Bytes[0x44],                               # ulong0
      Bytes[0x98] + uuid.bytes.to_slice,         # uuid
      Bytes[0xa0, 3, 1, 2, 3],                   # binary
    }
    ids.each_with_index do |id, i|
      correlation_id = ids[(i + 2) % ids.size]
      fields = IO::Memory.new
      fields.write id
      4.times { fields.write_byte 0x40_u8 } # user-id, to, subject, reply-to
      fields.write correlation_id
      payload = IO::Memory.new
      LavinMQ::AMQP10::Codec.write_descriptor(payload, LavinMQ::AMQP10::Descriptor::PROPERTIES)
      LavinMQ::AMQP10::Codec.write_list_header(payload, fields.size, 6)
      payload.write fields.to_slice
      LavinMQ::AMQP10::Codec.write_descriptor(payload, LavinMQ::AMQP10::Descriptor::DATA)
      LavinMQ::AMQP10::Codec.write_binary(payload, "body".to_slice)

      sections = delivered_sections(stored_message(payload.to_slice))
      sections.map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::PROPERTIES, LavinMQ::AMQP10::Descriptor::DATA]
      reader = IO::Memory.new(sections[0][1])
      LavinMQ::AMQP10::Codec.read_descriptor_code(reader)
      LavinMQ::AMQP10::Codec.read_list_header(reader)
      values = Array(Bytes).new
      6.times do
        start = reader.pos
        LavinMQ::AMQP10::Codec.skip_value(reader)
        values << sections[0][1][start, reader.pos - start]
      end
      values[0].should eq id
      values[5].should eq correlation_id
    end
  end

  it "delivers message annotations the way they were published" do
    annotations = LavinMQ::AMQP10::Value.map([
      {LavinMQ::AMQP10::Value.symbol("x-opt-a1"), LavinMQ::AMQP10::Value.long(12345_i64)},
      {LavinMQ::AMQP10::Value.symbol("x-opt-reason"), LavinMQ::AMQP10::Value.string("x" * 300)},
    ])
    section = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_value(section, LavinMQ::AMQP10::Value.described(
      LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::MESSAGE_ANNOTATIONS), annotations))
    payload = IO::Memory.new
    payload.write section.to_slice
    LavinMQ::AMQP10::Codec.write_descriptor(payload, LavinMQ::AMQP10::Descriptor::DATA)
    LavinMQ::AMQP10::Codec.write_binary(payload, "body".to_slice)

    msg = stored_message(payload.to_slice)
    sections = delivered_sections(msg, redelivered: true)

    sections.map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::HEADER, LavinMQ::AMQP10::Descriptor::MESSAGE_ANNOTATIONS,
                                   LavinMQ::AMQP10::Descriptor::DATA]
    sections[1][1].should eq section.to_slice
  end

  it "merges modified annotations into the stored ones, replacing equal keys" do
    base = encoded(annotations_map({"x-opt-a" => LavinMQ::AMQP10::Value.long(1_i64), "x-opt-b" => LavinMQ::AMQP10::Value.long(2_i64)}))
    update = encoded(annotations_map({"x-opt-b" => LavinMQ::AMQP10::Value.string("new"), "x-opt-c" => LavinMQ::AMQP10::Value.null}))

    merged = annotation_entries(LavinMQ::AMQP10::MessageCodec.merge_annotations(base, update))

    merged.keys.sort!.should eq ["x-opt-a", "x-opt-b", "x-opt-c"]
    merged["x-opt-a"].int?.should eq 1_i64
    merged["x-opt-b"].string?.should eq "new"
    annotation_entries(LavinMQ::AMQP10::MessageCodec.merge_annotations(nil, update)).size.should eq 2
  end

  it "treats differently encoded equal annotation keys as the same key" do
    base = Bytes[0xc1, 6, 2, 0xa3, 1, 'k'.ord, 0x55, 1]            # {sym8 k => 1}
    update = Bytes[0xc1, 9, 2, 0xb3, 0, 0, 0, 1, 'k'.ord, 0x55, 2] # {sym32 k => 2}
    merged = annotation_entries(LavinMQ::AMQP10::MessageCodec.merge_annotations(base, update))
    merged.size.should eq 1
    merged["k"].int?.should eq 2_i64
  end

  it "does not store null or empty message annotations" do
    [LavinMQ::AMQP10::Value.null, LavinMQ::AMQP10::Value.map(Array(Tuple(LavinMQ::AMQP10::Value, LavinMQ::AMQP10::Value)).new)].each do |value|
      payload = IO::Memory.new
      LavinMQ::AMQP10::Codec.write_value(payload, LavinMQ::AMQP10::Value.described(
        LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::MESSAGE_ANNOTATIONS), value))
      LavinMQ::AMQP10::Codec.write_descriptor(payload, LavinMQ::AMQP10::Descriptor::DATA)
      LavinMQ::AMQP10::Codec.write_binary(payload, "body".to_slice)
      stored_message(payload.to_slice).properties.headers.should be_nil
    end
  end

  it "does not deliver an annotations header that is not an encoded map" do
    headers = AMQ::Protocol::Table.new({"x-amqp10-message-annotations" => Bytes[0xc1, 9, 1]})
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", AMQ::Protocol::Properties.new(headers: headers), 4_u64, "body".to_slice)
    delivered_sections(msg).map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::DATA]
  end

  it "reports redeliveries in the header section's delivery-count" do
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", AMQ::Protocol::Properties.new, 4_u64, "body".to_slice)
    delivered_sections(msg).map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::DATA]

    sections = delivered_sections(msg, redelivered: true)
    sections[0][0].should eq LavinMQ::AMQP10::Descriptor::HEADER
    header = section_fields(sections[0][1])
    header[0].bool?.should be_false # durable
    header[4].uint?.should eq 1_u64 # delivery-count

    # A queue with a delivery-limit tracks the exact count.
    props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8, headers: AMQ::Protocol::Table.new({"x-delivery-count" => 3}))
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", props, 4_u64, "body".to_slice)
    header = section_fields(delivered_sections(msg, redelivered: true)[0][1])
    header[0].bool?.should be_true
    header[4].uint?.should eq 3_u64
  end

  it "delivers a published first-acquirer true until the message is redelivered" do
    [true, false].each do |first_acquirer|
      payload = IO::Memory.new
      LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::HEADER, [
        LavinMQ::AMQP10::Value.bool(false), LavinMQ::AMQP10::Value.null, LavinMQ::AMQP10::Value.null,
        LavinMQ::AMQP10::Value.bool(first_acquirer),
      ])
      LavinMQ::AMQP10::Codec.write_descriptor(payload, LavinMQ::AMQP10::Descriptor::DATA)
      LavinMQ::AMQP10::Codec.write_binary(payload, "body".to_slice)
      msg = stored_message(payload.to_slice)

      sections = delivered_sections(msg)
      if first_acquirer
        sections[0][0].should eq LavinMQ::AMQP10::Descriptor::HEADER
        header = section_fields(sections[0][1])
        header.size.should eq 4
        header[3].bool?.should be_true
      else
        sections.map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::DATA]
      end

      header = section_fields(delivered_sections(msg, redelivered: true)[0][1])
      header[3].null?.should be_true
      header[4].uint?.should eq 1_u64
    end
  end

  it "sizes fragmented redeliveries including the header section" do
    body = "x" * 1200
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", AMQ::Protocol::Properties.new(priority: 3_u8),
      body.bytesize.to_u64, body.to_slice)
    io = IO::Memory.new
    written, _frames = LavinMQ::AMQP10::MessageCodec.write_transfer(io, 0_u16, 0_u32, 7_u32,
      "tag".to_slice, msg, LavinMQ::AMQP10::MIN_MAX_FRAME_SIZE, redelivered: true)
    written.should eq io.size
    payload = IO::Memory.new
    bytes = io.to_slice
    offset = 0
    while offset < bytes.bytesize
      frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[offset, 4])
      reader = IO::Memory.new(bytes[offset + 8, frame_size.to_i - 8])
      LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
      payload.write reader.peek
      offset += frame_size.to_i
    end
    sections = message_sections(payload.to_slice)
    sections.map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::HEADER, LavinMQ::AMQP10::Descriptor::DATA]
    section_fields(sections[0][1])[4].uint?.should eq 1_u64
  end

  it "delivers a stored id as a string when it does not parse as its type" do
    headers = AMQ::Protocol::Table.new({"x-amqp10-message-id-type" => "uuid", "x-amqp10-correlation-id-type" => "ulong"})
    props = AMQ::Protocol::Properties.new(message_id: "not-a-uuid", correlation_id: "-1", headers: headers)
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", props, 4_u64, "body".to_slice)
    reader = IO::Memory.new(delivered_sections(msg)[0][1])
    LavinMQ::AMQP10::Codec.read_descriptor_code(reader)
    LavinMQ::AMQP10::Codec.read_list_header(reader)
    LavinMQ::AMQP10::Codec.read_string_value(reader).should eq "not-a-uuid"
    4.times { LavinMQ::AMQP10::Codec.skip_value(reader) }
    LavinMQ::AMQP10::Codec.read_string_value(reader).should eq "-1"
  end

  it "delivers a message published without a body section without one" do
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::PROPERTIES,
      [LavinMQ::AMQP10::Value.string("id")])
    msg = stored_message(payload.to_slice)
    msg.bodysize.should eq 0
    delivered_sections(msg).map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::PROPERTIES]

    # An empty data section, or an empty 0-9-1 body, is still delivered as data.
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_descriptor(payload, LavinMQ::AMQP10::Descriptor::DATA)
    LavinMQ::AMQP10::Codec.write_binary(payload, Bytes.empty)
    delivered_sections(stored_message(payload.to_slice)).map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::DATA]
  end

  it "delivers amqp-sequence bodies as they were published" do
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::PROPERTIES,
      [LavinMQ::AMQP10::Value.string("id")])
    sequences = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_described_list(sequences, LavinMQ::AMQP10::Descriptor::AMQP_SEQUENCE,
      [LavinMQ::AMQP10::Value.int(1), LavinMQ::AMQP10::Value.string("two")])
    LavinMQ::AMQP10::Codec.write_described_list(sequences, LavinMQ::AMQP10::Descriptor::AMQP_SEQUENCE,
      [LavinMQ::AMQP10::Value.null])
    payload.write sequences.to_slice
    msg = stored_message(payload.to_slice)
    msg.properties.headers.not_nil!["x-amqp10-body-type"].should eq "sequence"

    sections = delivered_sections(msg)
    sections.map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::PROPERTIES,
                                   LavinMQ::AMQP10::Descriptor::AMQP_SEQUENCE, LavinMQ::AMQP10::Descriptor::AMQP_SEQUENCE]
    (sections[1][1] + sections[2][1]).should eq sequences.to_slice
  end

  it "keeps 0-9-1 friendly bodies for string and binary amqp-values" do
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_value(payload, LavinMQ::AMQP10::Value.described(
      LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::AMQP_VALUE), LavinMQ::AMQP10::Value.string("text")))
    msg = stored_message(payload.to_slice)
    String.new(msg.body).should eq "text"
    msg.properties.headers.not_nil!["x-amqp10-body-type"].should eq "string"
  end

  it "delivers data bodies, including those published over 0-9-1, as a data section" do
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", AMQ::Protocol::Properties.new(headers: AMQ::Protocol::Table.new({"app" => "x"})),
      4_u64, "body".to_slice)
    delivered_sections(msg).map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::APPLICATION_PROPERTIES, LavinMQ::AMQP10::Descriptor::DATA]
  end

  it "ignores internal headers set by an AMQP 1.0 publisher" do
    payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_value(payload, LavinMQ::AMQP10::Value.described(
      LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::APPLICATION_PROPERTIES),
      LavinMQ::AMQP10::Value.map([{LavinMQ::AMQP10::Value.string("x-amqp10-body-type"), LavinMQ::AMQP10::Value.string("value")}])))
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::DATA)
    LavinMQ::AMQP10::Codec.write_binary(payload, "body".to_slice)

    stored_message(payload.to_slice).properties.headers.should be_nil
  end

  it "fragments outgoing transfers to the negotiated frame max" do
    body = "x" * 1200
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", AMQ::Protocol::Properties.new,
      body.bytesize.to_u64, body.to_slice)
    io = IO::Memory.new

    written, frames = LavinMQ::AMQP10::MessageCodec.write_transfer(io, 0_u16, 0_u32, 7_u32,
      "tag".to_slice, msg, LavinMQ::AMQP10::MIN_MAX_FRAME_SIZE)

    written.should eq io.size
    payload = IO::Memory.new
    more = [] of Bool
    delivery_ids = [] of UInt32?
    bytes = io.to_slice
    offset = 0
    while offset < bytes.bytesize
      frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[offset, 4])
      frame_size.should be <= LavinMQ::AMQP10::MIN_MAX_FRAME_SIZE
      frame_body = bytes[offset + 8, frame_size.to_i - 8]
      reader = IO::Memory.new(frame_body)
      transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
      more << transfer.more
      delivery_ids << transfer.delivery_id
      payload.write reader.peek
      offset += frame_size.to_i
    end

    frames.should eq more.size
    more.size.should be > 1
    more.first.should be_true
    more.last.should be_false
    delivery_ids.first.should eq 7_u32
    delivery_ids[1..].all?(Nil).should be_true
    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))
    String.new(incoming.body).should eq body
  end

  it "fragments outgoing transfers when message sections span frames" do
    headers = AMQ::Protocol::Table.new
    16.times do |i|
      headers["header-#{i}"] = "value-#{i}-#{"x" * 80}"
    end
    props = AMQ::Protocol::Properties.new
    props.headers = headers
    props.content_type = "text/plain"
    body = "body"
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", props, body.bytesize.to_u64, body.to_slice)
    io = IO::Memory.new

    written, _frames = LavinMQ::AMQP10::MessageCodec.write_transfer(io, 0_u16, 0_u32, 7_u32,
      "tag".to_slice, msg, LavinMQ::AMQP10::MIN_MAX_FRAME_SIZE)

    written.should eq io.size
    payload = IO::Memory.new
    more = [] of Bool
    delivery_ids = [] of UInt32?
    bytes = io.to_slice
    offset = 0
    while offset < bytes.bytesize
      frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[offset, 4])
      frame_size.should be <= LavinMQ::AMQP10::MIN_MAX_FRAME_SIZE
      frame_body = bytes[offset + 8, frame_size.to_i - 8]
      reader = IO::Memory.new(frame_body)
      transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
      more << transfer.more
      delivery_ids << transfer.delivery_id
      payload.write reader.peek
      offset += frame_size.to_i
    end

    more.size.should be > 2
    more.first.should be_true
    more.last.should be_false
    delivery_ids.first.should eq 7_u32
    delivery_ids[1..].all?(Nil).should be_true
    incoming = LavinMQ::AMQP10::MessageCodec.decode(IO::Memory.new(payload.to_slice))
    String.new(incoming.body).should eq body
    incoming.properties.content_type.should eq "text/plain"
    incoming.properties.headers.not_nil!["header-0"].should eq "value-0-#{"x" * 80}"
    incoming.properties.headers.not_nil!["header-15"].should eq "value-15-#{"x" * 80}"
  end

  it "writes AMQP 0-9-1 timestamps as AMQP 1.0 milliseconds" do
    timestamp = 1_700_000_000_i64
    props = AMQ::Protocol::Properties.new(timestamp: timestamp)
    body = "body"
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", props, body.bytesize.to_u64, body.to_slice)
    io = IO::Memory.new

    LavinMQ::AMQP10::MessageCodec.write_transfer(io, 0_u16, 0_u32, 7_u32, "tag".to_slice, msg)

    bytes = io.to_slice
    frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[0, 4])
    reader = IO::Memory.new(bytes[8, frame_size.to_i - 8])
    LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
    incoming = LavinMQ::AMQP10::MessageCodec.decode(reader)

    incoming.properties.timestamp_raw.should eq timestamp
  end

  it "clamps out-of-range 0-9-1 timestamps instead of overflowing" do
    body = "body"
    io = IO::Memory.new
    {Int64::MAX, Int64::MIN}.each do |timestamp|
      props = AMQ::Protocol::Properties.new(timestamp: timestamp)
      msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", props, body.bytesize.to_u64, body.to_slice)
      io.clear

      LavinMQ::AMQP10::MessageCodec.write_transfer(io, 0_u16, 0_u32, 7_u32, "tag".to_slice, msg)

      bytes = io.to_slice
      frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[0, 4])
      reader = IO::Memory.new(bytes[8, frame_size.to_i - 8])
      LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
      incoming = LavinMQ::AMQP10::MessageCodec.decode(reader)
      String.new(incoming.body).should eq body
    end
  end

  it "writes a header section carrying durable, priority and ttl on delivery" do
    props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8, priority: 5_u8, expiration: "60000")
    body = "body"
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", props, body.bytesize.to_u64, body.to_slice)
    io = IO::Memory.new

    LavinMQ::AMQP10::MessageCodec.write_transfer(io, 0_u16, 0_u32, 7_u32, "tag".to_slice, msg)

    bytes = io.to_slice
    frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[0, 4])
    reader = IO::Memory.new(bytes[8, frame_size.to_i - 8])
    LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
    incoming = LavinMQ::AMQP10::MessageCodec.decode(reader)

    incoming.properties.delivery_mode.should eq 2_u8
    incoming.properties.priority.should eq 5_u8
    incoming.properties.expiration.should eq "60000"
    String.new(incoming.body).should eq body
  end

  it "marks transfers settled when requested" do
    body = "body"
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", AMQ::Protocol::Properties.new, body.bytesize.to_u64, body.to_slice)
    io = IO::Memory.new

    LavinMQ::AMQP10::MessageCodec.write_transfer(io, 0_u16, 0_u32, 7_u32, "tag".to_slice, msg, settled: true)

    bytes = io.to_slice
    frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[0, 4])
    reader = IO::Memory.new(bytes[8, frame_size.to_i - 8])
    transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)

    transfer.settled.should be_true
  end
end

describe LavinMQ::AMQP10::Address do
  it "percent-decodes address components with URI decoding" do
    LavinMQ::AMQP10::Address.parse_source("/queues/my%2Fqueue").should eq "my/queue"
    target = LavinMQ::AMQP10::Address.parse_target("/exchanges/amq.direct/a%20b").not_nil!
    target.routing_key.should eq "a b"
  end
end

describe LavinMQ::AMQP10 do
  it "keeps AMQP 0-9-1 working on the same port" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("same-port-091", auto_delete: true)
        q.name.should eq "same-port-091"
      end
    end
  end

  it "logs in loopback clients that skip SASL as the default user" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s), mechanism: nil)
      conn = wait_for { s.connections.first?.as?(LavinMQ::AMQP10::Client) }
      conn.user.name.should eq "guest"
      conn.auth_mechanism.should eq "ANONYMOUS"
      client.close
    end
  end

  it "requires SASL from other than loopback, and once the default password is changed" do
    with_amqp_server do |s|
      remote = LavinMQ::ConnectionInfo.new(Socket::IPAddress.new("192.0.2.1", 5000), Socket::IPAddress.new("192.0.2.2", 5672))
      loopback = Socket::IPAddress.new("127.0.0.1", 0)
      proxied = LavinMQ::ConnectionInfo.new(loopback, loopback, proxied: true)
      factory_without_sasl(s, remote).should eq LavinMQ::AMQP10::SASL_HEADER
      factory_without_sasl(s, proxied).should eq LavinMQ::AMQP10::SASL_HEADER
      factory_without_sasl(s, LavinMQ::ConnectionInfo.local).should eq LavinMQ::AMQP10::PROTOCOL_HEADER

      s.users["guest"].update_password("changed")
      factory_without_sasl(s, LavinMQ::ConnectionInfo.local).should eq LavinMQ::AMQP10::SASL_HEADER
    ensure
      s.try &.users["guest"].update_password("guest")
    end
  end

  it "authenticates with SASL PLAIN" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s))
      should_eventually(eq "AMQP 1.0") { s.connections.first.details_tuple[:protocol] }
      client.close
    end
  end

  it "accepts the AMQP transport header split across reads after SASL" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s), split_transport_header: true)
      should_eventually(eq "AMQP 1.0") { s.connections.first.details_tuple[:protocol] }
      client.close
    end
  end

  it "clears the handshake read timeout when no idle-timeout is negotiated" do
    heartbeat = LavinMQ::Config.instance.heartbeat
    LavinMQ::Config.instance.heartbeat = 0_u16
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s))
      conn = wait_for { s.connections.first?.as?(LavinMQ::AMQP10::Client) }
      conn.@socket.as(TCPSocket).read_timeout.should be_nil
      client.close
    end
  ensure
    LavinMQ::Config.instance.heartbeat = heartbeat.not_nil!
  end

  it "keeps a read timeout for the idle check when the peer announces an idle-timeout" do
    heartbeat = LavinMQ::Config.instance.heartbeat
    LavinMQ::Config.instance.heartbeat = 0_u16
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s), idle_timeout: 4000_u32)
      conn = wait_for { s.connections.first?.as?(LavinMQ::AMQP10::Client) }
      conn.@socket.as(TCPSocket).read_timeout.should eq 2.seconds
      client.close
    end
  ensure
    LavinMQ::Config.instance.heartbeat = heartbeat.not_nil!
  end

  it "tears down idle connections after management close" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s))
      conn = wait_for { s.connections.first?.as?(LavinMQ::AMQP10::Client) }

      conn.close("spec close", 50.milliseconds)

      should_eventually(eq 0) { s.connections.size }
      client.close
    end
  end

  it "fails bad SASL PLAIN authentication" do
    with_amqp_server do |s|
      AMQP10SpecClient.authenticate(amqp_port(s), "guest", "wrong").should eq 1
    end
  end

  it "authenticates loopback connections with SASL ANONYMOUS as the default user" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s), mechanism: "ANONYMOUS")
      conn = wait_for { s.connections.first?.as?(LavinMQ::AMQP10::Client) }
      conn.user.name.should eq "guest"
      conn.auth_mechanism.should eq "ANONYMOUS"
      client.close

      factory_sasl(s, LavinMQ::ConnectionInfo.local, "ANONYMOUS")
        .should eq({["PLAIN", "ANONYMOUS"], 0_u8})
    end
  end

  it "neither offers nor accepts SASL ANONYMOUS from other than loopback" do
    with_amqp_server do |s|
      remote = LavinMQ::ConnectionInfo.new(Socket::IPAddress.new("192.0.2.1", 5000), Socket::IPAddress.new("192.0.2.2", 5672))
      factory_sasl(s, remote, "ANONYMOUS").should eq({["PLAIN"], 1_u8})
      loopback = Socket::IPAddress.new("127.0.0.1", 0)
      proxied = LavinMQ::ConnectionInfo.new(loopback, loopback, proxied: true)
      factory_sasl(s, proxied, "ANONYMOUS").should eq({["PLAIN"], 1_u8})
    end
  end

  it "refuses SASL ANONYMOUS once the default user's password is changed" do
    with_amqp_server do |s|
      s.users["guest"].update_password("changed")
      factory_sasl(s, LavinMQ::ConnectionInfo.local, "ANONYMOUS")[1].should eq 1
      factory_sasl(s, LavinMQ::ConnectionInfo.local, "PLAIN", "\0guest\0changed".to_slice)[1].should eq 0
    ensure
      s.try &.users["guest"].update_password("guest")
    end
  end

  it "does not count proxied connections from loopback as loopback for the default user" do
    with_amqp_server do |s|
      loopback = Socket::IPAddress.new("127.0.0.1", 0)
      proxied = LavinMQ::ConnectionInfo.new(loopback, loopback, proxied: true)
      factory_sasl(s, proxied, "PLAIN", "\0guest\0guest".to_slice)[1].should eq 1
      factory_sasl(s, LavinMQ::ConnectionInfo.local, "PLAIN", "\0guest\0guest".to_slice)[1].should eq 0
    end
  end

  it "splits SASL PLAIN responses on bytes regardless of the authzid encoding" do
    with_amqp_server do |s|
      AMQP10SpecClient.authenticate(amqp_port(s), "guest", "guest", authzid: "gäst\xff").should eq 0
      AMQP10SpecClient.authenticate(amqp_port(s), "guest", "wrong", authzid: "gäst\xff").should eq 1
    end
  end

  it "advertises channel-max and refuses sessions on channels above it" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s), expect_open: false)
      open = LavinMQ::AMQP10::Open.from_value(client.read_value)
      channel_max = open.channel_max.not_nil!
      channel_max.should eq LavinMQ::Config.instance.channel_max

      client.begin_session(channel: channel_max)
      client.read_performative_code.should eq LavinMQ::AMQP10::Descriptor::BEGIN

      client.begin_session(channel: channel_max + 1)
      close = client.read_value
      close.descriptor_code?.should eq LavinMQ::AMQP10::Descriptor::CLOSE
      client.close
    end
  end

  it "sends open before close when refusing the vhost" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s), hostname: "vhost:missing", expect_open: false)
      client.read_performative_code.should eq LavinMQ::AMQP10::Descriptor::OPEN
      close = client.read_value
      close.descriptor_code?.should eq LavinMQ::AMQP10::Descriptor::CLOSE
      client.error_fields(close)[0].symbol?.should eq LavinMQ::AMQP10::ErrorCondition::NOT_FOUND
      client.close
    end
  end

  it "selects vhost from prefixed open hostname" do
    with_amqp_server do |s|
      s.vhosts.create("amqp10-vhost")
      s.users.add_permission("guest", "amqp10-vhost", /.*/, /.*/, /.*/)
      client = AMQP10SpecClient.new(amqp_port(s), hostname: "vhost:amqp10-vhost")
      should_eventually(eq "amqp10-vhost") { s.connections.first.details_tuple[:vhost] }
      client.close
    end
  end

  it "publishes to a queue address consumable by AMQP 0-9-1" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-q", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}")
        client.publish(0_u32, 1_u32, "hello").should eq LavinMQ::AMQP10::Outcome::Accepted
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.gets_to_end.should eq "hello"
        client.close
      end
    end
  end

  it "reassembles fragmented transfers before publishing" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-fragmented", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}")
        client.publish_fragmented(0_u32, 1_u32, "hello fragmented").should eq LavinMQ::AMQP10::Outcome::Accepted
        msg = q.get(no_ack: true).not_nil!
        msg.body_io.gets_to_end.should eq "hello fragmented"
        client.close
      end
    end
  end

  it "ignores empty keepalive frames without closing the connection" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-keepalive", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}")
        client.send_empty_frame
        client.publish(0_u32, 1_u32, "after-keepalive").should eq LavinMQ::AMQP10::Outcome::Accepted
        q.get(no_ack: true).not_nil!.body_io.gets_to_end.should eq "after-keepalive"
        client.close
      end
    end
  end

  it "sends keepalives while a peer keeps publishing pre-settled transfers" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-keepalive-busy", auto_delete: true)
        # The peer expects a frame from us at least every 200 ms; we should send
        # one after 100 ms of silence even though its transfers keep arriving.
        client = AMQP10SpecClient.new(amqp_port(s), idle_timeout: 200_u32)
        client.attach_sender("/queues/#{q.name}")
        done = Channel(Nil).new
        spawn do
          40.times do |i|
            client.publish_settled(0_u32, i.to_u32, "busy")
            sleep 20.milliseconds
          end
          done.send nil
        end

        frame = client.reader.read
        frame.body.empty?.should be_true
        done.receive
        should_eventually(eq 40) { s.vhosts["/"].queue(q.name).message_count }
        client.close
      end
    end
  end

  it "closes the connection on frames larger than its max-frame-size" do
    frame_max = LavinMQ::Config.instance.frame_max
    LavinMQ::Config.instance.frame_max = 4096_u32
    with_amqp_server do |s|
      # The peer's larger max-frame-size limits only what the server sends.
      client = AMQP10SpecClient.new(amqp_port(s), frame_max: 65_536_u32)
      # The limit counts the 8 byte frame header as well.
      client.send_junk_frame(4097)

      close = client.read_value
      close.descriptor_code?.should eq LavinMQ::AMQP10::Descriptor::CLOSE
      client.error_fields(close)[0].symbol?.should eq LavinMQ::AMQP10::ErrorCondition::DECODE_ERROR
      client.error_fields(close)[1].string?.should eq "AMQP 1.0 frame too large 4097"
      client.close
    end
  ensure
    LavinMQ::Config.instance.frame_max = frame_max.not_nil!
  end

  it "advertises its own max-frame-size and accepts frames up to it from a peer with a smaller one" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-own-frame-max", auto_delete: true)
        frame_max = LavinMQ::AMQP10::MIN_MAX_FRAME_SIZE
        client = AMQP10SpecClient.new(amqp_port(s), frame_max: frame_max, expect_open: false)
        open = LavinMQ::AMQP10::Open.from_value(client.read_value)
        open.max_frame_size.should eq LavinMQ::Config.instance.frame_max

        client.begin_session
        client.read_performative_code.should eq LavinMQ::AMQP10::Descriptor::BEGIN
        client.attach_sender("/queues/#{q.name}")
        body = "x" * 2000 # a frame larger than the peer's own max-frame-size
        client.publish(0_u32, 1_u32, body).should eq LavinMQ::AMQP10::Outcome::Accepted
        q.get(no_ack: true).not_nil!.body_io.gets_to_end.should eq body
        client.close
      end
    end
  end

  it "accepts frames above the handshake buffer size when frame_max is unlimited" do
    frame_max = LavinMQ::Config.instance.frame_max
    LavinMQ::Config.instance.frame_max = 0_u32
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-unlimited-frame-max", auto_delete: true)
        # frame_max 0 is unlimited, so the peer's 4096 wins; the Open was read
        # with a minimum-size buffer that must not cap the connection.
        client = AMQP10SpecClient.new(amqp_port(s), frame_max: 4096_u32)
        client.attach_sender("/queues/#{q.name}")
        body = "x" * 2000
        client.publish(0_u32, 1_u32, body).should eq LavinMQ::AMQP10::Outcome::Accepted
        q.get(no_ack: true).not_nil!.body_io.gets_to_end.should eq body
        client.close
      end
    end
  ensure
    LavinMQ::Config.instance.frame_max = frame_max.not_nil!
  end

  it "rejects oversized fragmented publishes before the final frame" do
    LavinMQ::Config.instance.max_message_size = 16
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-fragment-limit", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}")
        client.publish_oversized_fragment(0_u32, 1_u32, 17).should eq LavinMQ::AMQP10::Outcome::Rejected
        q.get(no_ack: true).should be_nil
        client.close
      end
    end
  end

  it "reports the sender's delivery-count and replenishes link credit" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-credit-refill", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}", initial_delivery_count: 5_u32)
        flow = client.attach_flow.not_nil!
        flow.delivery_count.should eq 5_u32
        flow.link_credit.should eq LavinMQ::AMQP10::ReceiverLink::LINK_CREDIT

        # Credit is topped up once half of it has been used: the last of these
        # deliveries is the one that triggers it.
        used = LavinMQ::AMQP10::ReceiverLink::LINK_CREDIT // 2
        used.times { |i| client.publish_settled(0_u32, i.to_u32, "credit") }
        _flows, outcome = client.publish_reading_flows(0_u32, used, "credit")
        outcome.should eq LavinMQ::AMQP10::Outcome::Accepted

        refill = client.read_flow
        refill.handle.should eq 0_u32
        refill.delivery_count.should eq 5_u32 + used + 1
        refill.link_credit.should eq LavinMQ::AMQP10::ReceiverLink::LINK_CREDIT
        should_eventually(eq used.to_i + 1) { s.vhosts["/"].queue(q.name).message_count }
        client.close
      end
    end
  end

  it "closes the connection on a first transfer frame without delivery-id" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-no-delivery-id", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}")
        client.write_transfer_without_delivery_id(0_u32, "no-id")

        close = client.read_value
        close.descriptor_code?.should eq LavinMQ::AMQP10::Descriptor::CLOSE
        client.error_fields(close)[0].symbol?.should eq LavinMQ::AMQP10::ErrorCondition::DECODE_ERROR
        q.get(no_ack: true).should be_nil
        client.close
      end
    end
  end

  it "routes exchange targets and anonymous sender messages" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q1 = ch.queue("amqp10-ex-q", auto_delete: true)
        q2 = ch.queue("amqp10-anon-q", auto_delete: true)
        ex = ch.exchange("amqp10-ex", "direct", auto_delete: true)
        q1.bind(ex.name, "rk")
        q2.bind(ex.name, "anon")

        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/exchanges/#{ex.name}/rk")
        client.publish(0_u32, 1_u32, "via-exchange").should eq LavinMQ::AMQP10::Outcome::Accepted
        q1.get(no_ack: true).not_nil!.body_io.gets_to_end.should eq "via-exchange"

        client.attach_sender(nil, handle: 1_u32, name: "anonymous")
        client.publish(1_u32, 2_u32, "via-to", "/exchanges/#{ex.name}/anon").should eq LavinMQ::AMQP10::Outcome::Accepted
        q2.get(no_ack: true).not_nil!.body_io.gets_to_end.should eq "via-to"
        client.close
      end
    end
  end

  it "releases unroutable publishes and rejects invalid publish addresses" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ex = ch.exchange("amqp10-unroutable", "direct", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/exchanges/#{ex.name}/missing")
        client.publish(0_u32, 1_u32, "unroutable").should eq LavinMQ::AMQP10::Outcome::Released

        client.attach_sender(nil, handle: 1_u32, name: "anonymous")
        client.publish(1_u32, 2_u32, "bad-to", "/queue/not-v2").should eq LavinMQ::AMQP10::Outcome::Rejected
        client.close
      end
    end
  end

  it "consumes AMQP 0-9-1 publishes from a queue source" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-consume", auto_delete: true)
        q.publish("from-091")
        client = AMQP10SpecClient.new(amqp_port(s))
        attach = client.attach_receiver("/queues/#{q.name}")
        attach.initial_delivery_count.should eq 0_u32
        client.flow
        transfer, incoming = client.consume_one_delivery
        transfer.delivery_id.should eq 0_u32
        String.new(incoming.body).should eq "from-091"
        should_eventually(eq 0) { s.vhosts["/"].queue(q.name).message_count }
        client.close
      end
    end
  end

  it "drains unused credit on an empty queue and echoes a flow" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-drain-empty", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow(credit: 5_u32, drain: true)

        flow = client.read_flow
        flow.link_credit.should eq 0_u32
        flow.delivery_count.should eq 5_u32
        flow.drain.should be_true
        client.close
      end
    end
  end

  it "delivers available messages then drains remaining credit" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-drain-partial", auto_delete: true)
        q.publish("only-one")
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow(credit: 5_u32, drain: true)

        transfer, incoming = client.read_delivery
        String.new(incoming.body).should eq "only-one"
        client.settle(transfer.delivery_id.not_nil!)

        flow = client.read_flow
        flow.link_credit.should eq 0_u32
        flow.delivery_count.should eq 5_u32
        flow.drain.should be_true
        client.close
      end
    end
  end

  it "echoes a flow with the current credit when echo is requested" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-flow-echo", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow(credit: 3_u32, echo: true)

        flow = client.read_flow
        flow.link_credit.should eq 3_u32
        flow.drain.should be_false
        client.expect_no_frame
        client.close
      end
    end
  end

  it "uses unique delivery ids across sender links in a session" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q1 = ch.queue("amqp10-multi-link-1", auto_delete: true)
        q2 = ch.queue("amqp10-multi-link-2", auto_delete: true)
        q1.publish("one")
        q2.publish("two")
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q1.name}", handle: 0_u32, name: "receiver-1")
        client.attach_receiver("/queues/#{q2.name}", handle: 1_u32, name: "receiver-2")
        client.flow(handle: 0_u32)
        client.flow(handle: 1_u32)

        first = client.consume_one_delivery[0].delivery_id.not_nil!
        second = client.consume_one_delivery[0].delivery_id.not_nil!

        [first, second].sort.should eq [0_u32, 1_u32]
        client.close
      end
    end
  end

  it "stops delivering when the peer's session incoming-window is exhausted" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-remote-window", auto_delete: true)
        internal_q = s.vhosts["/"].queue(q.name)
        q.publish("one")
        q.publish("two")
        should_eventually(eq 2) { internal_q.message_count }
        client = AMQP10SpecClient.new(amqp_port(s), incoming_window: 1_u32)
        client.attach_receiver("/queues/#{q.name}")
        client.flow(credit: 2_u32)

        client.consume_one.should eq "one"
        client.expect_no_frame
        internal_q.message_count.should eq 1

        # We have received one transfer, so our next-incoming-id is 1; make room for one more.
        client.session_flow(next_incoming_id: 1_u32, incoming_window: 1_u32)
        client.consume_one.should eq "two"
        client.close
      end
    end
  end

  it "ends a session closed via the management API and discards its in-flight frames" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-session-close", auto_delete: true)
        internal_q = s.vhosts["/"].queue(q.name)
        q.publish("unacked")
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow
        client.read_delivery
        should_eventually(eq 1) { internal_q.unacked_count }

        amqp10_session(s).close("closed by admin")

        end_frame = client.read_value
        end_frame.descriptor_code?.should eq LavinMQ::AMQP10::Descriptor::END
        error = client.error_fields(end_frame)
        error[0].symbol?.should eq LavinMQ::AMQP10::ErrorCondition::PRECONDITION_FAILED
        error[1].string?.should eq "closed by admin"
        should_eventually(eq 1) { internal_q.message_count }
        internal_q.unacked_count.should eq 0

        # Sent before the peer processed our end: must be ignored, not treated
        # as a frame on an unknown session.
        client.flow
        client.end_session
        client.expect_no_frame

        # The channel can be reused once the end exchange is complete.
        client.begin_session
        client.read_performative_code.should eq LavinMQ::AMQP10::Descriptor::BEGIN
        client.close
      end
    end
  end

  it "includes channel details for AMQP 1.0 consumers" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-consumer-details", auto_delete: true)
        internal_q = s.vhosts["/"].queue(q.name)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")

        consumer = wait_for { internal_q.consumers.first? }
        details = JSON.parse(consumer.to_json)

        details["channel_details"]["connection_name"].as_s.should_not be_empty
        details["channel_details"]["number"].as_i.should eq 0
        details["channel_details"]["name"].as_s.should_not be_empty
        client.close
      end
    end
  end

  it "deletes queues with idle AMQP 1.0 consumers" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-delete-idle-consumer")
        internal_q = s.vhosts["/"].queue(q.name)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow

        internal_q.delete.should be_true
        should_eventually(be_nil) { s.vhosts["/"].queue?(q.name) }
        client.close
      end
    end
  end

  it "honors single active consumer for AMQP 1.0 sender links" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        args = AMQP::Client::Arguments.new({"x-single-active-consumer" => true})
        q = ch.queue("amqp10-sac", auto_delete: true, args: args)
        internal_q = s.vhosts["/"].queue(q.name)
        q.publish("one")
        q.publish("two")
        first = AMQP10SpecClient.new(amqp_port(s))
        second = AMQP10SpecClient.new(amqp_port(s))
        first.attach_receiver("/queues/#{q.name}")
        second.attach_receiver("/queues/#{q.name}")

        first.flow(credit: 1_u32)
        second.flow(credit: 1_u32)
        first.consume_one.should eq "one"
        second.expect_no_frame
        internal_q.message_count.should eq 1
        first.detach

        second.consume_one.should eq "two"
        first.close
        second.close
      end
    end
  end

  it "does not deliver from paused queues to AMQP 1.0 sender links" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-paused", auto_delete: true)
        internal_q = s.vhosts["/"].queue(q.name)
        internal_q.pause!
        q.publish("paused")
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow

        client.expect_no_frame
        internal_q.unacked_count.should eq 0
        internal_q.resume!

        client.consume_one.should eq "paused"
        client.close
      end
    end
  end

  it "waits behind higher priority consumers for AMQP 1.0 sender links" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-priority", auto_delete: true)
        internal_q = s.vhosts["/"].queue(q.name)
        higher_priority = AMQP10PrioritySpecConsumer.new(internal_q)
        internal_q.add_consumer(higher_priority)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow
        q.publish("priority")

        client.expect_no_frame
        internal_q.message_count.should eq 1
        internal_q.unacked_count.should eq 0
        higher_priority.close

        client.consume_one.should eq "priority"
        client.close
      end
    end
  end

  it "does not deliver to AMQP 1.0 sender links while vhost flow is stopped" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-server-flow", auto_delete: true)
        internal_q = s.vhosts["/"].queue(q.name)
        # The 0-9-1 publish is asynchronous: make sure the message is in the
        # queue and the flow is stopped before the link exists, so the deliver
        # loop cannot observe any other order of events.
        q.publish("flow")
        should_eventually(eq 1) { internal_q.message_count }
        s.flow(false)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow

        client.expect_no_frame
        internal_q.unacked_count.should eq 0
        s.flow(true)

        client.consume_one.should eq "flow"
        client.close
      ensure
        s.flow(true)
      end
    end
  end

  it "detaches AMQP 1.0 sender links on consumer timeout" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        args = AMQP::Client::Arguments.new({"x-consumer-timeout" => 100})
        q = ch.queue("amqp10-consumer-timeout", auto_delete: true, args: args)
        internal_q = s.vhosts["/"].queue(q.name)
        q.publish("timeout")
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow

        _transfer, incoming = client.read_delivery
        String.new(incoming.body).should eq "timeout"
        detach = client.read_detach

        detach.closed.should be_true
        should_eventually(eq 1) { internal_q.message_count }
        should_eventually(eq 0) { internal_q.unacked_count }
        client.close
      end
    end
  end

  it "replenishes the session incoming window after incoming transfers" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-window-refill", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}")
        session = amqp10_session(s)
        session.@incoming_window_remaining.set(1_u32, :release)

        flows, outcome = client.publish_reading_flows(0_u32, 1_u32, "window")

        outcome.should eq LavinMQ::AMQP10::Outcome::Accepted
        flows.size.should eq 1
        # next-incoming-id counts received transfer frames from the peer's
        # initial next-outgoing-id (0), so it is 1 after a single-frame transfer.
        flows[0].next_incoming_id.should eq 1_u32
        flows[0].incoming_window.should eq LavinMQ::AMQP10::DEFAULT_WINDOW
        client.close
      end
    end
  end

  it "fragments consumed messages to the negotiated frame max" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        frame_max = LavinMQ::AMQP10::MIN_MAX_FRAME_SIZE
        body = "x" * (frame_max.to_i * 3)
        q = ch.queue("amqp10-consume-fragmented", auto_delete: true)
        q.publish(body)
        client = AMQP10SpecClient.new(amqp_port(s), frame_max: frame_max)
        client.attach_receiver("/queues/#{q.name}")
        client.flow
        client.consume_one_fragmented(max_frame_size: frame_max).should eq body
        client.close
      end
    end
  end

  it "delivers AMQP headers as AMQP 1.0 application-properties" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-headers", auto_delete: true)
        headers = AMQP::Client::Arguments.new({
          "app"     => "lavinmq",
          "enabled" => true,
          "tries"   => 3_i32,
          "ratio"   => 1.5_f64,
        })
        q.publish("with-headers", props: AMQP::Client::Properties.new(headers: headers))
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow
        incoming = client.consume_one_message
        String.new(incoming.body).should eq "with-headers"
        incoming.properties.headers.not_nil!["app"].should eq "lavinmq"
        incoming.properties.headers.not_nil!["enabled"].should be_true
        incoming.properties.headers.not_nil!["tries"].should eq 3_i32
        incoming.properties.headers.not_nil!["ratio"].should eq 1.5_f64
        client.close
      end
    end
  end

  it "uses link credit and applies accepted released rejected and modified dispositions" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-settle", auto_delete: true)
        internal_q = s.vhosts["/"].queue(q.name)
        q.publish("release-me")
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")

        sleep 20.milliseconds
        internal_q.message_count.should eq 1
        internal_q.unacked_count.should eq 0

        client.flow
        client.consume_one(LavinMQ::AMQP10::Outcome::Released).should eq "release-me"
        should_eventually(eq 1) { internal_q.message_count }
        should_eventually(eq 0) { internal_q.unacked_count }

        client.flow(delivery_count: 1_u32)
        client.consume_one.should eq "release-me"
        should_eventually(eq 0) { internal_q.message_count }
        should_eventually(eq 0) { internal_q.unacked_count }

        q.publish("drop-me")
        client.flow(delivery_count: 2_u32)
        client.consume_one(LavinMQ::AMQP10::Outcome::Rejected).should eq "drop-me"
        should_eventually(eq 0) { internal_q.message_count + internal_q.unacked_count }

        q.publish("modify-me")
        client.flow(delivery_count: 3_u32)
        client.consume_one(LavinMQ::AMQP10::Outcome::Modified).should eq "modify-me"
        should_eventually(eq 1) { internal_q.message_count }
        should_eventually(eq 0) { internal_q.unacked_count }

        client.flow(delivery_count: 4_u32)
        client.consume_one.should eq "modify-me"
        should_eventually(eq 0) { internal_q.message_count + internal_q.unacked_count }
        client.close
      end
    end
  end

  it "settles unsettled dispositions for rcv-settle-mode second receivers" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-rcv-second", auto_delete: true)
        internal_q = s.vhosts["/"].queue(q.name)
        q.publish("second")
        client = AMQP10SpecClient.new(amqp_port(s))
        attach = client.attach_receiver("/queues/#{q.name}", rcv_settle_mode: 1_u8)
        attach.rcv_settle_mode.should eq 1_u8
        client.flow
        transfer, incoming = client.read_delivery
        String.new(incoming.body).should eq "second"
        delivery_id = transfer.delivery_id.not_nil!
        client.settle(delivery_id, LavinMQ::AMQP10::Outcome::Accepted, settled: false)

        disposition = client.read_disposition
        disposition.role.should eq LavinMQ::AMQP10::Role::Sender
        disposition.first.should eq delivery_id
        disposition.settled.should be_true
        disposition.outcome.should eq LavinMQ::AMQP10::Outcome::Accepted
        should_eventually(eq 0) { internal_q.message_count + internal_q.unacked_count }
        client.close
      end
    end
  end

  it "leaves settling incoming deliveries to rcv-settle-mode second senders" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-rcv-settle-second", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}", rcv_settle_mode: 1_u8).rcv_settle_mode.should eq 1_u8
        client.write_publish(0_u32, 1_u32, "second")
        disposition = client.read_disposition
        disposition.outcome.should eq LavinMQ::AMQP10::Outcome::Accepted
        disposition.settled.should be_false
        # The sender settles; nothing is expected in reply.
        client.settle(1_u32, role: LavinMQ::AMQP10::Role::Sender)
        q.get(no_ack: true).not_nil!.body_io.gets_to_end.should eq "second"

        client.attach_sender("/queues/#{q.name}", handle: 1_u32, name: "first").rcv_settle_mode.should eq 0_u8
        client.write_publish(1_u32, 2_u32, "first")
        client.read_disposition.settled.should be_true
        client.close
      end
    end
  end

  it "advertises the settle modes actually in use on attach" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-settle-modes", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        attach = client.attach_sender("/queues/#{q.name}", snd_settle_mode: 2_u8, rcv_settle_mode: 1_u8)
        attach.snd_settle_mode.should eq 2_u8
        attach.rcv_settle_mode.should eq 1_u8
        # Mixed is not supported for deliveries: they are sent unsettled.
        attach = client.attach_receiver("/queues/#{q.name}", handle: 1_u32, name: "mixed", snd_settle_mode: 2_u8)
        attach.snd_settle_mode.should eq 0_u8
        attach = client.attach_receiver("/queues/#{q.name}", handle: 2_u32, name: "settled", snd_settle_mode: 1_u8)
        attach.snd_settle_mode.should eq 1_u8
        client.close
      end
    end
  end

  it "rejects attaches with an error condition matching the cause" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        s.users.create("amqp10-limited", "pw")
        s.users.add_permission("amqp10-limited", "/", /^$/, /^$/, /^$/)
        q = ch.queue("amqp10-attach-errors", auto_delete: true)
        not_found = LavinMQ::AMQP10::ErrorCondition::NOT_FOUND
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender_detached("/queues/missing", handle: 0_u32)
        client.last_detach_condition.should eq not_found
        client.attach_sender_detached("/exchanges/missing/rk", handle: 1_u32)
        client.last_detach_condition.should eq not_found
        client.attach_receiver_detached("/queues/missing", handle: 2_u32)
        client.last_detach_condition.should eq not_found
        # A bare node name resolves to no node.
        client.attach_sender_detached("f47ac10b-58cc-4372-a567-0e02b2c3d479", handle: 3_u32)
        client.last_detach_condition.should eq not_found
        client.attach_receiver_detached("f47ac10b-58cc-4372-a567-0e02b2c3d479", handle: 4_u32)
        client.last_detach_condition.should eq not_found
        client.attach_receiver_detached("/queues/#{q.name}", handle: 5_u32, name: "durable", durable: 2_u32)
        client.last_detach_condition.should eq LavinMQ::AMQP10::ErrorCondition::NOT_IMPLEMENTED
        client.close

        limited = AMQP10SpecClient.new(amqp_port(s), username: "amqp10-limited", password: "pw")
        limited.attach_receiver_detached("/queues/#{q.name}")
        limited.last_detach_condition.should eq LavinMQ::AMQP10::ErrorCondition::UNAUTHORIZED_ACCESS
        limited.close

        owner = AMQP10SpecClient.new(amqp_port(s))
        address = owner.attach_sender(nil, dynamic: true).target.not_nil!.address.not_nil!
        other = AMQP10SpecClient.new(amqp_port(s))
        other.attach_receiver_detached(address)
        other.last_detach_condition.should eq LavinMQ::AMQP10::ErrorCondition::RESOURCE_LOCKED
        other.close
        owner.close
      end
    end
  end

  it "ignores frames on rejected links until the peer detaches them" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-rejected-link-frames", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        # Clients typically issue credit right after attaching, before they see
        # the server's detach of a rejected link.
        client.attach_receiver_detached("/queues/missing", handle: 0_u32)
        client.flow(handle: 0_u32)
        client.attach_sender_detached("/queues/missing", handle: 1_u32)
        client.publish_settled(1_u32, 1_u32, "to-nowhere")
        client.send_detach(0_u32)
        client.send_detach(1_u32)
        client.expect_no_frame

        # The connection is still usable and the handles can be reused.
        client.attach_sender("/queues/#{q.name}", handle: 0_u32)
        client.publish(0_u32, 2_u32, "after").should eq LavinMQ::AMQP10::Outcome::Accepted
        client.close
      end
    end
  end

  it "sends a header with delivery-count when a released message is redelivered" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-redelivery-header", auto_delete: true)
        q.publish("again")
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        client.flow(credit: 2_u32)
        transfer, sections = client.read_delivery_sections
        sections.map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::DATA]
        client.settle(transfer.delivery_id.not_nil!, LavinMQ::AMQP10::Outcome::Released)

        transfer, sections = client.read_delivery_sections
        sections[0][0].should eq LavinMQ::AMQP10::Descriptor::HEADER
        section_fields(sections[0][1])[4].uint?.should eq 1_u64
        client.settle(transfer.delivery_id.not_nil!)
        client.close
      end
    end
  end

  it "accepts and delivers amqp-value bodies of every fixed-width type" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-fixed-width-values", auto_delete: true)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}")
        client.attach_receiver("/queues/#{q.name}", handle: 1_u32)
        client.flow(handle: 1_u32, credit: 100_u32)
        # short (0x61) was once rejected as an unsupported value
        {0x61 => 2, 0x60 => 2, 0x73 => 4, 0x74 => 4, 0x84 => 8, 0x94 => 16, 0x98 => 16}.each_with_index do |(code, width), i|
          message = IO::Memory.new
          LavinMQ::AMQP10::Codec.write_descriptor(message, LavinMQ::AMQP10::Descriptor::AMQP_VALUE)
          message.write_byte code.to_u8
          width.times { |b| message.write_byte (b + 1).to_u8 }
          client.publish_raw(0_u32, i.to_u32, message.to_slice).should eq LavinMQ::AMQP10::Outcome::Accepted
          transfer, sections = client.read_delivery_sections
          sections.should eq [{LavinMQ::AMQP10::Descriptor::AMQP_VALUE, message.to_slice}]
          client.settle(transfer.delivery_id.not_nil!)
        end
        client.close
      end
    end
  end

  it "applies modified annotations to the message's later deliveries" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("amqp10-modified-annotations", auto_delete: true)
        internal_q = s.vhosts["/"].queue(q.name)
        message = IO::Memory.new
        LavinMQ::AMQP10::Codec.write_value(message, LavinMQ::AMQP10::Value.described(
          LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::MESSAGE_ANNOTATIONS),
          annotations_map({"x-opt-a1" => LavinMQ::AMQP10::Value.long(12345_i64)})))
        LavinMQ::AMQP10::Codec.write_descriptor(message, LavinMQ::AMQP10::Descriptor::DATA)
        LavinMQ::AMQP10::Codec.write_binary(message, "body".to_slice)
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_sender("/queues/#{q.name}")
        client.publish_raw(0_u32, 1_u32, message.to_slice).should eq LavinMQ::AMQP10::Outcome::Accepted
        client.attach_receiver("/queues/#{q.name}", handle: 1_u32)
        client.flow(handle: 1_u32, credit: 10_u32)

        transfer, _sections = client.read_delivery_sections
        client.settle_modified(transfer.delivery_id.not_nil!,
          annotations_map({"x-opt-reason" => LavinMQ::AMQP10::Value.string("app offline")}))
        transfer, _sections = client.read_delivery_sections
        client.settle_modified(transfer.delivery_id.not_nil!,
          annotations_map({"x-opt-retry" => LavinMQ::AMQP10::Value.long(2_i64)}))

        transfer, sections = client.read_delivery_sections
        sections.map(&.[0]).should eq [LavinMQ::AMQP10::Descriptor::HEADER, LavinMQ::AMQP10::Descriptor::MESSAGE_ANNOTATIONS,
                                       LavinMQ::AMQP10::Descriptor::DATA]
        annotations = sections[1][1]
        entries = annotation_entries(annotations[3, annotations.size - 3]) # skip the descriptor
        entries["x-opt-a1"].int?.should eq 12345_i64
        entries["x-opt-reason"].string?.should eq "app offline"
        entries["x-opt-retry"].int?.should eq 2_i64
        client.settle(transfer.delivery_id.not_nil!)
        should_eventually(eq 0) { internal_q.message_count + internal_q.unacked_count }
        internal_q.@header_overrides.not_nil!.empty?.should be_true
        client.close
      end
    end
  end

  it "creates and deletes dynamic source queues on detach" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s))
      attach = client.attach_receiver(nil, dynamic: true)
      address = attach.source.not_nil!.address.not_nil!
      queue_name = address.split("/").last
      s.vhosts["/"].queue?(queue_name).should_not be_nil
      client.detach
      should_eventually(be_nil) { s.vhosts["/"].queue?(queue_name) }
      client.close
    end
  end

  it "creates exclusive auto-delete dynamic target queues" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s))
      attach = client.attach_sender(nil, dynamic: true)
      address = attach.target.not_nil!.address.not_nil!
      queue_name = address.split("/").last
      queue = s.vhosts["/"].queue?(queue_name).should be_a LavinMQ::AMQP::Queue
      queue.exclusive?.should be_true
      queue.auto_delete?.should be_true
      queue.durable?.should be_false

      client.publish(0_u32, 1_u32, "dynamic").should eq LavinMQ::AMQP10::Outcome::Accepted
      queue.message_count.should eq 1

      other = AMQP10SpecClient.new(amqp_port(s))
      other.attach_receiver_detached(address).closed.should be_true
      other.close

      client.detach
      should_eventually(be_nil) { s.vhosts["/"].queue?(queue_name) }
      client.close
    end
  end

  it "rejects unsupported topology and terminus requests without creating queues" do
    with_amqp_server do |s|
      client = AMQP10SpecClient.new(amqp_port(s))
      client.attach_sender_detached("/queue/v1-auto-create").closed.should be_true
      s.vhosts["/"].queue?("v1-auto-create").should be_nil

      client.attach_receiver_detached("$management").closed.should be_true
      dynamic_props = LavinMQ::AMQP10::Value.map([
        {LavinMQ::AMQP10::Value.symbol("x-queue-type"), LavinMQ::AMQP10::Value.string("quorum")},
      ])
      client.attach_receiver_detached(nil, name: "dynamic-props", dynamic: true,
        dynamic_node_properties: dynamic_props).closed.should be_true
      client.attach_sender_detached(nil, name: "durable-target", dynamic: true,
        durable: 1_u32).closed.should be_true
      client.close
    end
  end
end
