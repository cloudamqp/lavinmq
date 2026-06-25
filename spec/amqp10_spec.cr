require "./spec_helper"

private class AMQP10SpecClient
  getter io, reader

  def initialize(port : Int32, username = "guest", password = "guest", hostname : String? = nil,
                 frame_max = LavinMQ::Config.instance.frame_max, split_transport_header = false)
    @io = TCPSocket.new("localhost", port)
    @io.read_timeout = 5.seconds
    @reader = LavinMQ::AMQP10::FrameReader.new(@io, LavinMQ::Config.instance.frame_max)
    sasl_handshake(username, password)
    send_transport_header(split_transport_header)
    send_open(hostname, frame_max)
    read_performative_code.should eq LavinMQ::AMQP10::Descriptor::OPEN
    send_begin
    begin_frame = LavinMQ::AMQP10::Begin.from_value(read_value)
    begin_frame.remote_channel.should eq 0_u16
  end

  def self.authenticate(port : Int32, username, password) : UInt8
    io = TCPSocket.new("localhost", port)
    io.read_timeout = 5.seconds
    reader = LavinMQ::AMQP10::FrameReader.new(io, LavinMQ::Config.instance.frame_max)
    io.write LavinMQ::AMQP10::SASL_HEADER
    io.flush
    header = Bytes.new(8)
    io.read_fully(header)
    header.should eq LavinMQ::AMQP10::SASL_HEADER
    reader.read
    response = "\0#{username}\0#{password}"
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

  def attach_sender(address : String?, handle = 0_u32, name = "sender", dynamic = false) : LavinMQ::AMQP10::Attach
    target = LavinMQ::AMQP10::Target.new(address, dynamic: dynamic).to_value
    fields = attach_fields(name, handle, role_receiver: false,
      source: LavinMQ::AMQP10::Value.null, target: target)
    send_performative(LavinMQ::AMQP10::Descriptor::ATTACH, fields)
    attach = LavinMQ::AMQP10::Attach.from_value(read_value)
    flow = LavinMQ::AMQP10::Flow.from_value(read_value)
    flow.next_incoming_id.should_not be_nil
    flow.incoming_window.should_not be_nil
    flow.next_outgoing_id.should_not be_nil
    flow.outgoing_window.should_not be_nil
    flow.link_credit.should eq Int32::MAX.to_u32
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
    detach = LavinMQ::AMQP10::Detach.from_value(read_value)
    detach.handle.should eq attach.handle
    detach
  end

  def attach_receiver(address : String?, handle = 0_u32, name = "receiver", dynamic = false) : LavinMQ::AMQP10::Attach
    source = LavinMQ::AMQP10::Source.new(address, dynamic: dynamic).to_value
    fields = attach_fields(name, handle, role_receiver: true,
      source: source, target: LavinMQ::AMQP10::Value.null)
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
    detach = LavinMQ::AMQP10::Detach.from_value(read_value)
    detach.handle.should eq attach.handle
    detach
  end

  def flow(handle = 0_u32, credit = 1_u32, delivery_count = 0_u32) : Nil
    fields = Array(LavinMQ::AMQP10::Value).new(7)
    4.times { fields << LavinMQ::AMQP10::Value.null }
    fields << LavinMQ::AMQP10::Value.uint(handle)
    fields << LavinMQ::AMQP10::Value.uint(delivery_count)
    fields << LavinMQ::AMQP10::Value.uint(credit)
    send_performative(LavinMQ::AMQP10::Descriptor::FLOW, fields)
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
      code = LavinMQ::AMQP10::MessageCodec.read_descriptor_code(frame.body_reader)
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
    LavinMQ::AMQP10::TransferCodec.write_transfer_performative(first, handle, delivery_id, tag, true)
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

  def publish_oversized_fragment(handle : UInt32, delivery_id : UInt32, payload_size : Int32) : LavinMQ::AMQP10::Outcome
    payload = IO::Memory.new
    tag = delivery_id.to_s.to_slice
    LavinMQ::AMQP10::TransferCodec.write_transfer_performative(payload, handle, delivery_id, tag, true)
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

  def read_delivery
    frame = @reader.read
    reader = frame.body_reader
    transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
    incoming = LavinMQ::AMQP10::MessageCodec.decode(reader)
    {transfer, incoming}
  end

  def settle(delivery_id : UInt32, outcome = LavinMQ::AMQP10::Outcome::Accepted) : Nil
    LavinMQ::AMQP10::TransferCodec.write_disposition(@io, 0_u16,
      delivery_id, outcome)
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
      payload.write reader.remaining_slice
      break unless transfer.more
    end
    incoming = LavinMQ::AMQP10::MessageCodec.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))
    LavinMQ::AMQP10::TransferCodec.write_disposition(@io, 0_u16, delivery_id.not_nil!, outcome)
    String.new(incoming.body)
  end

  def detach(handle = 0_u32) : Nil
    fields = [LavinMQ::AMQP10::Value.uint(handle), LavinMQ::AMQP10::Value.bool(true)]
    send_performative(LavinMQ::AMQP10::Descriptor::DETACH, fields)
    read_performative_code.should eq LavinMQ::AMQP10::Descriptor::DETACH
  end

  private def sasl_handshake(username, password)
    @io.write LavinMQ::AMQP10::SASL_HEADER
    @io.flush
    header = Bytes.new(8)
    @io.read_fully(header)
    header.should eq LavinMQ::AMQP10::SASL_HEADER
    @reader.read # sasl-mechanisms
    response = "\0#{username}\0#{password}"
    fields = [LavinMQ::AMQP10::Value.symbol("PLAIN"), LavinMQ::AMQP10::Value.binary(response.to_slice)]
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

  private def send_open(hostname, frame_max)
    fields = [LavinMQ::AMQP10::Value.string("spec-client")]
    fields << (hostname ? LavinMQ::AMQP10::Value.string(hostname) : LavinMQ::AMQP10::Value.null)
    fields << LavinMQ::AMQP10::Value.uint(frame_max)
    send_performative(LavinMQ::AMQP10::Descriptor::OPEN, fields)
  end

  private def send_begin
    fields = [
      LavinMQ::AMQP10::Value.null,
      LavinMQ::AMQP10::Value.uint(0_u32),
      LavinMQ::AMQP10::Value.uint(LavinMQ::AMQP10::DEFAULT_WINDOW),
      LavinMQ::AMQP10::Value.uint(LavinMQ::AMQP10::DEFAULT_WINDOW),
    ]
    send_performative(LavinMQ::AMQP10::Descriptor::BEGIN, fields)
  end

  private def attach_fields(name, handle, role_receiver, source, target)
    fields = Array(LavinMQ::AMQP10::Value).new(7)
    fields << LavinMQ::AMQP10::Value.string(name)
    fields << LavinMQ::AMQP10::Value.uint(handle)
    fields << LavinMQ::AMQP10::Value.bool(role_receiver)
    fields << LavinMQ::AMQP10::Value.null
    fields << LavinMQ::AMQP10::Value.null
    fields << source
    fields << target
    fields
  end

  private def send_performative(code, fields)
    LavinMQ::AMQP10::FrameWriter.write_performative(@io, 0_u16,
      LavinMQ::AMQP10::AMQP_FRAME_TYPE, code, fields)
  end

  private def write_publish(handle : UInt32, delivery_id : UInt32, body : String, to : String? = nil) : Nil
    payload = IO::Memory.new
    tag = delivery_id.to_s.to_slice
    LavinMQ::AMQP10::TransferCodec.write_transfer_performative(payload, handle, delivery_id, tag, false)
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

  private def read_performative_code
    read_value.described?.not_nil!.descriptor_code?
  end

  private def read_value
    LavinMQ::AMQP10::Codec.decode(@reader.read.body_reader)
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

    incoming = LavinMQ::AMQP10::MessageCodec.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))

    incoming.properties.delivery_mode.should eq 2_u8
    incoming.properties.priority.should eq 7_u8
    incoming.properties.headers.not_nil!["app"].should eq "amqp10"
    incoming.properties.headers.not_nil!["enabled"].should eq true
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
    incoming = LavinMQ::AMQP10::MessageCodec.decode(LavinMQ::AMQP10::SliceReader.new(string_payload.to_slice))
    String.new(incoming.body).should eq "value-body"

    binary_payload = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_value(binary_payload,
      LavinMQ::AMQP10::Value.described(
        LavinMQ::AMQP10::Value.ulong(LavinMQ::AMQP10::Descriptor::AMQP_VALUE),
        LavinMQ::AMQP10::Value.binary("binary-body".to_slice)
      )
    )
    incoming = LavinMQ::AMQP10::MessageCodec.decode(LavinMQ::AMQP10::SliceReader.new(binary_payload.to_slice))
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

    incoming = LavinMQ::AMQP10::MessageCodec.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))

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

    incoming = LavinMQ::AMQP10::MessageCodec.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))

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
      LavinMQ::AMQP10::MessageCodec.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))
    end
  end
end

describe LavinMQ::AMQP10::Value do
  it "keeps values compact enough for inline scalar storage" do
    sizeof(LavinMQ::AMQP10::Value).should be <= 32
  end
end

describe LavinMQ::AMQP10::Codec do
  it "decodes one-byte signed integer values" do
    value = LavinMQ::AMQP10::Codec.decode(LavinMQ::AMQP10::SliceReader.new(Bytes[0x51_u8, 0xff_u8]))

    value.int?.should eq -1_i64

    value = LavinMQ::AMQP10::Codec.decode(LavinMQ::AMQP10::SliceReader.new(Bytes[0x54_u8, 0xff_u8]))
    value.int?.should eq -1_i64

    value = LavinMQ::AMQP10::Codec.decode(LavinMQ::AMQP10::SliceReader.new(Bytes[0x55_u8, 0xff_u8]))
    value.int?.should eq -1_i64
  end

  it "preserves float and double values" do
    io = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_value(io, LavinMQ::AMQP10::Value.float(1.25_f32))
    value = LavinMQ::AMQP10::Codec.decode(LavinMQ::AMQP10::SliceReader.new(io.to_slice))
    value.float?.should eq 1.25_f32

    io.clear
    LavinMQ::AMQP10::Codec.write_value(io, LavinMQ::AMQP10::Value.double(1.5_f64))
    value = LavinMQ::AMQP10::Codec.decode(LavinMQ::AMQP10::SliceReader.new(io.to_slice))
    value.double?.should eq 1.5_f64
  end

  it "includes the constructor byte in array8 encoded size" do
    io = IO::Memory.new
    LavinMQ::AMQP10::Codec.write_value(io, LavinMQ::AMQP10::Value.array([LavinMQ::AMQP10::Value.symbol("PLAIN")]))

    io.to_slice.should eq Bytes[0xe0_u8, 0x08_u8, 0x01_u8, 0xa3_u8, 0x05_u8,
      0x50_u8, 0x4c_u8, 0x41_u8, 0x49_u8, 0x4e_u8]
  end

  it "rejects compound counts larger than the encoded payload" do
    payload = Bytes[0xd0_u8, 0_u8, 0_u8, 0_u8, 4_u8, 0x7f_u8, 0xff_u8, 0xff_u8, 0xff_u8]

    expect_raises(LavinMQ::AMQP10::DecodeError) do
      LavinMQ::AMQP10::Codec.decode(LavinMQ::AMQP10::SliceReader.new(payload))
    end
  end

  it "raises DecodeError for oversized variable-width values" do
    payload = IO::Memory.new
    payload.write_byte 0xb1_u8
    LavinMQ::AMQP10::Codec.write_u32(payload, UInt32::MAX)

    expect_raises(LavinMQ::AMQP10::DecodeError) do
      LavinMQ::AMQP10::Codec.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))
    end
  end

  it "rejects excessively nested described values" do
    payload = IO::Memory.new
    80.times do
      payload.write_byte 0x00_u8
      payload.write_byte 0x43_u8
    end
    payload.write_byte 0x40_u8

    expect_raises(LavinMQ::AMQP10::DecodeError) do
      LavinMQ::AMQP10::Codec.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))
    end
  end
end

describe LavinMQ::AMQP10::TransferCodec do
  it "decodes transfer aborted from field 9" do
    {true, false}.each do |aborted|
      payload = IO::Memory.new
      fields = [
        LavinMQ::AMQP10::Value.uint(1_u32),
        LavinMQ::AMQP10::Value.null,
        LavinMQ::AMQP10::Value.null,
        LavinMQ::AMQP10::Value.null,
        LavinMQ::AMQP10::Value.null,
        LavinMQ::AMQP10::Value.null,
        LavinMQ::AMQP10::Value.null,
        LavinMQ::AMQP10::Value.null,
        LavinMQ::AMQP10::Value.null,
        LavinMQ::AMQP10::Value.bool(aborted),
        LavinMQ::AMQP10::Value.bool(!aborted),
      ]
      LavinMQ::AMQP10::Codec.write_described_list(payload, LavinMQ::AMQP10::Descriptor::TRANSFER, fields)

      transfer = LavinMQ::AMQP10::Transfer.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))

      transfer.aborted.should eq aborted
    end
  end

  it "writes mandatory session fields in flow frames" do
    io = IO::Memory.new

    written = LavinMQ::AMQP10::TransferCodec.write_flow(io, 3_u16, 11_u32, 22_u32, 33_u32, 44_u32,
      5_u32, 6_u32, 7_u32)

    written.should eq io.size
    bytes = io.to_slice
    frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[0, 4])
    reader = LavinMQ::AMQP10::SliceReader.new(bytes[8, frame_size.to_i - 8])
    flow = LavinMQ::AMQP10::Flow.from_value(LavinMQ::AMQP10::Codec.decode(reader))

    flow.next_incoming_id.should eq 11_u32
    flow.incoming_window.should eq 22_u32
    flow.next_outgoing_id.should eq 33_u32
    flow.outgoing_window.should eq 44_u32
    flow.handle.should eq 5_u32
    flow.delivery_count.should eq 6_u32
    flow.link_credit.should eq 7_u32
  end

  it "raises DecodeError for out-of-range uint fields" do
    payload = IO::Memory.new
    payload.write_byte 0x00_u8
    LavinMQ::AMQP10::Codec.write_ulong(payload, LavinMQ::AMQP10::Descriptor::TRANSFER)
    payload.write_byte 0xc0_u8
    payload.write_byte 10_u8
    payload.write_byte 1_u8
    payload.write_byte 0x80_u8
    LavinMQ::AMQP10::Codec.write_u64(payload, UInt64::MAX)

    expect_raises(LavinMQ::AMQP10::DecodeError) do
      LavinMQ::AMQP10::TransferCodec.read_transfer(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))
    end
  end

  it "fragments outgoing transfers to the negotiated frame max" do
    body = "x" * 1200
    msg = LavinMQ::BytesMessage.new(1_i64, "", "rk", AMQ::Protocol::Properties.new,
      body.bytesize.to_u64, body.to_slice)
    io = IO::Memory.new

    written = LavinMQ::AMQP10::TransferCodec.write_transfer(io, 0_u16, 0_u32, 7_u32,
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
      reader = LavinMQ::AMQP10::SliceReader.new(frame_body)
      transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
      more << transfer.more
      delivery_ids << transfer.delivery_id
      payload.write reader.remaining_slice
      offset += frame_size.to_i
    end

    more.size.should be > 1
    more.first.should be_true
    more.last.should be_false
    delivery_ids.first.should eq 7_u32
    delivery_ids[1..].all?(Nil).should be_true
    incoming = LavinMQ::AMQP10::MessageCodec.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))
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

    written = LavinMQ::AMQP10::TransferCodec.write_transfer(io, 0_u16, 0_u32, 7_u32,
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
      reader = LavinMQ::AMQP10::SliceReader.new(frame_body)
      transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
      more << transfer.more
      delivery_ids << transfer.delivery_id
      payload.write reader.remaining_slice
      offset += frame_size.to_i
    end

    more.size.should be > 2
    more.first.should be_true
    more.last.should be_false
    delivery_ids.first.should eq 7_u32
    delivery_ids[1..].all?(Nil).should be_true
    incoming = LavinMQ::AMQP10::MessageCodec.decode(LavinMQ::AMQP10::SliceReader.new(payload.to_slice))
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

    LavinMQ::AMQP10::TransferCodec.write_transfer(io, 0_u16, 0_u32, 7_u32, "tag".to_slice, msg)

    bytes = io.to_slice
    frame_size = IO::ByteFormat::NetworkEndian.decode(UInt32, bytes[0, 4])
    reader = LavinMQ::AMQP10::SliceReader.new(bytes[8, frame_size.to_i - 8])
    LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
    incoming = LavinMQ::AMQP10::MessageCodec.decode(reader)

    incoming.properties.timestamp_raw.should eq timestamp
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

  it "rejects bare AMQP 1.0 transport by advertising SASL" do
    with_amqp_server do |s|
      io = TCPSocket.new("localhost", amqp_port(s))
      io.write LavinMQ::AMQP10::PROTOCOL_HEADER
      io.flush
      header = Bytes.new(8)
      io.read_fully(header)
      header.should eq LavinMQ::AMQP10::SASL_HEADER
      eof = uninitialized UInt8[1]
      io.read(eof.to_slice).should eq 0
      io.close
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
        q.publish("flow")
        client = AMQP10SpecClient.new(amqp_port(s))
        client.attach_receiver("/queues/#{q.name}")
        s.flow(false)
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
        flows[0].next_incoming_id.should eq 2_u32
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
        incoming.properties.headers.not_nil!["enabled"].should eq true
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
