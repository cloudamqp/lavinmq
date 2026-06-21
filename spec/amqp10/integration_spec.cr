require "../spec_helper"
require "../../src/lavinmq/amqp10/frames"
require "../../src/lavinmq/amqp10/messaging"
require "socket"

# A minimal AMQP 1.0 client built on LavinMQ's own (round-trip-tested) codec,
# used to drive the server through a full handshake -> publish -> consume ->
# settle cycle end to end. Topology is declared out-of-band via AMQP 0-9-1
# (the explicit-declaration policy), then exercised over AMQP 1.0.
class AMQP10TestClient
  include LavinMQ::AMQP10

  def initialize(port : Int32)
    @socket = TCPSocket.new("localhost", port)
    @socket.read_timeout = 5.seconds
    @socket.sync = true
  end

  def close
    @socket.close rescue nil
  end

  def connect(user = "guest", password = "guest", hostname = "/")
    # SASL layer
    @socket.write SASL_PROTOCOL_HEADER
    read_header # server SASL header
    read_until(Descriptor::SASL_MECHANISMS)
    response = IO::Memory.new
    response.write_byte 0_u8
    response << user
    response.write_byte 0_u8
    response << password
    FrameWriter.write(@socket, type: Frame::TYPE_SASL) do |b|
      # sasl-init: mechanism, initial-response, hostname
      fl = FieldList.new
      fl.symbol "PLAIN"
      fl.binary response.to_slice
      fl.null
      Codec.write_described_list(b, Descriptor::SASL_INIT, fl.fields)
    end
    _, outcome = read_until(Descriptor::SASL_OUTCOME)
    code = FieldReader.from(outcome).u8?(255_u8)
    raise "SASL failed (code #{code})" unless code == Sasl::OK

    # AMQP layer
    @socket.write AMQP_PROTOCOL_HEADER
    read_header
    FrameWriter.write(@socket) { |b| Open.new(container_id: "test", hostname: hostname).to_io(b) }
    read_until(Descriptor::OPEN)
    FrameWriter.write(@socket) do |b|
      Begin.new(next_outgoing_id: 0_u32, incoming_window: 100_u32, outgoing_window: 100_u32).to_io(b)
    end
    read_until(Descriptor::BEGIN)
  end

  # Attach a sender link (we publish) to a target address.
  def attach_sender(handle : UInt32, target : String)
    FrameWriter.write(@socket) do |b|
      Attach.new(name: "snd-#{handle}", handle: handle, role: Attach::ROLE_SENDER,
        target: Target.new(address: target), initial_delivery_count: 0_u32).to_io(b)
    end
    read_until(Descriptor::ATTACH)
    read_until(Descriptor::FLOW) # broker grants us credit
  end

  def publish(handle : UInt32, delivery_id : UInt32, body : String)
    payload = MessageCodec.encode(AMQ::Protocol::Properties.new, body.to_slice)
    tag = Bytes.new(4)
    IO::ByteFormat::BigEndian.encode(delivery_id, tag)
    FrameWriter.write(@socket) do |b|
      Transfer.new(handle: handle, delivery_id: delivery_id, delivery_tag: tag, settled: false).to_io(b)
      b.write payload
    end
    _, disp = read_until(Descriptor::DISPOSITION)
    Disposition.decode(disp)
  end

  # Attach a receiver link (we consume) from a source address, then grant credit.
  def attach_receiver(handle : UInt32, source : String, credit : UInt32)
    FrameWriter.write(@socket) do |b|
      Attach.new(name: "rcv-#{handle}", handle: handle, role: Attach::ROLE_RECEIVER,
        source: Source.new(address: source)).to_io(b)
    end
    read_until(Descriptor::ATTACH)
    FrameWriter.write(@socket) do |b|
      Flow.new(incoming_window: 100_u32, next_outgoing_id: 0_u32, outgoing_window: 100_u32,
        next_incoming_id: 0_u32, handle: handle, delivery_count: 0_u32, link_credit: credit).to_io(b)
    end
  end

  # Attach a sender link with a dynamic source; returns the server-assigned address.
  def attach_dynamic_receiver(handle : UInt32) : String
    FrameWriter.write(@socket) do |b|
      Attach.new(name: "dyn-#{handle}", handle: handle, role: Attach::ROLE_RECEIVER,
        source: Source.new(dynamic: true)).to_io(b)
    end
    _, perf = read_until(Descriptor::ATTACH)
    Attach.decode(perf).source.not_nil!.address.not_nil!
  end

  # Receive one transfer, return {delivery_id, body}.
  def receive : {UInt32, String}
    frame, perf = read_until(Descriptor::TRANSFER)
    transfer = Transfer.decode(perf)
    parsed = MessageCodec.parse(frame.payload)
    {transfer.delivery_id.not_nil!, String.new(parsed.body)}
  end

  def accept(delivery_id : UInt32)
    FrameWriter.write(@socket) do |b|
      Disposition.new(role: Attach::ROLE_RECEIVER, first: delivery_id, settled: true,
        state: DeliveryState.accepted).to_io(b)
    end
  end

  private def read_header
    buf = Bytes.new(8)
    @socket.read_fully(buf)
    buf
  end

  private def read_until(descriptor : UInt64) : {Frame, Described}
    loop do
      frame = Frame.read(@socket)
      next if frame.heartbeat?
      perf = frame.performative
      next unless perf
      return {frame, perf} if perf.descriptor.as?(UInt64) == descriptor
    end
  end
end

describe "AMQP 1.0 end-to-end" do
  it "publishes to a queue and consumes it back with settlement" do
    with_amqp_server do |s|
      # Declare topology out-of-band via AMQP 0-9-1 (explicit-declaration policy).
      with_channel(s) do |ch|
        ch.queue("q10", durable: true)
      end

      client = AMQP10TestClient.new(amqp_port(s))
      begin
        client.connect
        client.attach_sender(0_u32, "/queues/q10")
        disp = client.publish(0_u32, 0_u32, "hello amqp 1.0")
        LavinMQ::AMQP10::DeliveryState.classify(disp.state)
          .should eq LavinMQ::AMQP10::DeliveryState::Outcome::Accepted

        wait_for { s.vhosts["/"].queue("q10").message_count == 1 }

        client.attach_receiver(1_u32, "/queues/q10", 10_u32)
        delivery_id, body = client.receive
        body.should eq "hello amqp 1.0"
        client.accept(delivery_id)

        # After settlement the queue should be empty (message acked).
        wait_for { s.vhosts["/"].queue("q10").message_count == 0 }
      ensure
        client.close
      end

      s.vhosts["/"].delete_queue("q10")
    end
  end

  it "implicitly creates an exclusive queue for a dynamic source (RabbitMQ 4.1)" do
    with_amqp_server do |s|
      client = AMQP10TestClient.new(amqp_port(s))
      begin
        client.connect
        address = client.attach_dynamic_receiver(0_u32)
        address.should_not be_empty
        # The server-named queue must now exist and be auto-delete.
        q = s.vhosts["/"].queue(address)
        q.should_not be_nil
        q.auto_delete?.should be_true
      ensure
        client.close
      end
    end
  end

  it "rejects attaching to a queue that was not declared (amqp:not-found)" do
    with_amqp_server do |s|
      client = AMQP10TestClient.new(amqp_port(s))
      begin
        client.connect
        # Attaching a sender to a missing queue target must be refused with a
        # closed detach carrying amqp:not-found.
        socket = client.@socket
        LavinMQ::AMQP10::FrameWriter.write(socket) do |b|
          LavinMQ::AMQP10::Attach.new(name: "snd", handle: 0_u32,
            role: LavinMQ::AMQP10::Attach::ROLE_SENDER,
            target: LavinMQ::AMQP10::Target.new(address: "/queues/missing"),
            initial_delivery_count: 0_u32).to_io(b)
        end
        # First the mirrored attach may arrive, then a detach with error.
        detach = nil
        10.times do
          frame = LavinMQ::AMQP10::Frame.read(socket)
          next if frame.heartbeat?
          perf = frame.performative
          next unless perf
          if perf.descriptor.as?(UInt64) == LavinMQ::AMQP10::Descriptor::DETACH
            detach = LavinMQ::AMQP10::Detach.decode(perf)
            break
          end
        end
        detach.should_not be_nil
        detach.not_nil!.error.not_nil!.condition.should eq LavinMQ::AMQP10::ErrorCondition::NOT_FOUND
      ensure
        client.close
      end
    end
  end
end
