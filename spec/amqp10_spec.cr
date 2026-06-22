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
    read_performative_code.should eq LavinMQ::AMQP10::Descriptor::BEGIN
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

  def attach_sender(address : String?, handle = 0_u32, name = "sender", dynamic = false) : LavinMQ::AMQP10::Attach
    target = LavinMQ::AMQP10::Target.new(address, dynamic: dynamic).to_value
    fields = attach_fields(name, handle, role_receiver: false,
      source: LavinMQ::AMQP10::Value.null, target: target)
    send_performative(LavinMQ::AMQP10::Descriptor::ATTACH, fields)
    attach = LavinMQ::AMQP10::Attach.from_value(read_value)
    read_performative_code.should eq LavinMQ::AMQP10::Descriptor::FLOW
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
    LavinMQ::AMQP10::Detach.from_value(read_value)
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
    LavinMQ::AMQP10::Detach.from_value(read_value)
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

    frame = @reader.read
    disposition = LavinMQ::AMQP10::TransferCodec.read_disposition(frame.body_reader)
    disposition.outcome.not_nil!
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
    frame = @reader.read
    reader = frame.body_reader
    transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
    incoming = LavinMQ::AMQP10::MessageCodec.decode(reader)
    LavinMQ::AMQP10::TransferCodec.write_disposition(@io, 0_u16,
      transfer.delivery_id.not_nil!, outcome)
    {transfer, incoming}
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
end

describe LavinMQ::AMQP10::Codec do
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
end

describe LavinMQ::AMQP10::TransferCodec do
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

  it "uses link credit and applies accepted released and rejected dispositions" do
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
