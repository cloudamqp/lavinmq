require "./spec_helper"

private class AMQP10SpecClient
  getter io, reader

  def initialize(port : Int32, username = "guest", password = "guest", hostname : String? = nil)
    @io = TCPSocket.new("localhost", port)
    @io.read_timeout = 5.seconds
    @reader = LavinMQ::AMQP10::FrameReader.new(@io, LavinMQ::Config.instance.frame_max)
    sasl_handshake(username, password)
    @io.write LavinMQ::AMQP10::PROTOCOL_HEADER
    @io.flush
    send_open(hostname)
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

  def consume_one(outcome = LavinMQ::AMQP10::Outcome::Accepted) : String
    frame = @reader.read
    reader = frame.body_reader
    transfer = LavinMQ::AMQP10::TransferCodec.read_transfer(reader)
    incoming = LavinMQ::AMQP10::MessageCodec.decode(reader)
    LavinMQ::AMQP10::TransferCodec.write_disposition(@io, 0_u16,
      transfer.delivery_id.not_nil!, outcome)
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

  private def send_open(hostname)
    fields = [LavinMQ::AMQP10::Value.string("spec-client")]
    fields << (hostname ? LavinMQ::AMQP10::Value.string(hostname) : LavinMQ::AMQP10::Value.null)
    fields << LavinMQ::AMQP10::Value.uint(LavinMQ::Config.instance.frame_max)
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

  private def read_performative_code
    read_value.described?.not_nil!.descriptor_code?
  end

  private def read_value
    LavinMQ::AMQP10::Codec.decode(@reader.read.body_reader)
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
        client.attach_receiver("/queues/#{q.name}")
        client.flow
        client.consume_one.should eq "from-091"
        should_eventually(eq 0) { s.vhosts["/"].queue(q.name).message_count }
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
