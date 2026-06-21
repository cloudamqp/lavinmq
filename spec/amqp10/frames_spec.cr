require "spec"
require "../../src/lavinmq/amqp10/frames"
require "../../src/lavinmq/amqp10/sasl"
require "../../src/lavinmq/amqp10/messaging"

include LavinMQ::AMQP10

# Encode a performative into a frame, read the frame back, return the Described.
private def frame_roundtrip(perf) : Frame
  io = IO::Memory.new
  FrameWriter.write(io) { |b| perf.to_io(b) }
  io.rewind
  Frame.read(io)
end

describe LavinMQ::AMQP10 do
  describe "frame layer" do
    it "round-trips an empty heartbeat frame" do
      io = IO::Memory.new
      FrameWriter.heartbeat(io)
      io.rewind
      f = Frame.read(io)
      f.heartbeat?.should be_true
      f.performative.should be_nil
    end

    it "carries the channel" do
      io = IO::Memory.new
      FrameWriter.write(io, channel: 7_u16) { |b| End.new.to_io(b) }
      io.rewind
      Frame.read(io).channel.should eq 7_u16
    end
  end

  describe "performatives" do
    it "round-trips open" do
      f = frame_roundtrip(Open.new(container_id: "lavinmq", hostname: "vh", idle_timeout: 60000_u32))
      o = Open.decode(f.performative.not_nil!)
      o.container_id.should eq "lavinmq"
      o.hostname.should eq "vh"
      o.idle_timeout.should eq 60000_u32
    end

    it "round-trips begin" do
      f = frame_roundtrip(Begin.new(next_outgoing_id: 0_u32, incoming_window: 100_u32, outgoing_window: 100_u32, remote_channel: 3_u16))
      b = Begin.decode(f.performative.not_nil!)
      b.remote_channel.should eq 3_u16
      b.incoming_window.should eq 100_u32
    end

    it "round-trips attach with source and target" do
      attach = Attach.new(name: "link-1", handle: 0_u32, role: Attach::ROLE_SENDER,
        source: Source.new(address: "/queues/q1"),
        target: Target.new(dynamic: true),
        initial_delivery_count: 0_u32)
      f = frame_roundtrip(attach)
      a = Attach.decode(f.performative.not_nil!)
      a.name.should eq "link-1"
      a.role?.should eq Attach::ROLE_SENDER
      a.source.not_nil!.address.should eq "/queues/q1"
      a.target.not_nil!.dynamic?.should be_true
      a.initial_delivery_count.should eq 0_u32
    end

    it "round-trips flow with link credit" do
      f = frame_roundtrip(Flow.new(incoming_window: 100_u32, next_outgoing_id: 0_u32, outgoing_window: 100_u32, handle: 0_u32, link_credit: 10_u32, delivery_count: 5_u32))
      fl = Flow.decode(f.performative.not_nil!)
      fl.handle.should eq 0_u32
      fl.link_credit.should eq 10_u32
      fl.delivery_count.should eq 5_u32
    end

    it "round-trips transfer fields" do
      f = frame_roundtrip(Transfer.new(handle: 0_u32, delivery_id: 42_u32, delivery_tag: Bytes[1, 2, 3], settled: false))
      t = Transfer.decode(f.performative.not_nil!)
      t.handle.should eq 0_u32
      t.delivery_id.should eq 42_u32
      t.delivery_tag.should eq Bytes[1, 2, 3]
      t.settled.should eq false
    end

    it "round-trips disposition with accepted outcome" do
      f = frame_roundtrip(Disposition.new(role: true, first: 1_u32, last: 3_u32, settled: true, state: DeliveryState.accepted))
      d = Disposition.decode(f.performative.not_nil!)
      d.first.should eq 1_u32
      d.last.should eq 3_u32
      d.settled?.should be_true
      DeliveryState.classify(d.state).should eq DeliveryState::Outcome::Accepted
    end

    it "round-trips detach and close with an error" do
      f = frame_roundtrip(Detach.new(handle: 0_u32, closed: true, error: ErrorPerformative.new(ErrorCondition::NOT_FOUND, "no such queue")))
      d = Detach.decode(f.performative.not_nil!)
      d.closed?.should be_true
      d.error.not_nil!.condition.should eq ErrorCondition::NOT_FOUND
      d.error.not_nil!.description.should eq "no such queue"
    end
  end

  describe "SASL" do
    it "decodes sasl-init carrying PLAIN credentials" do
      io = IO::Memory.new
      io.write_byte 0_u8
      io << "guest"
      io.write_byte 0_u8
      io << "guest"
      user, pass = Sasl.plain_credentials(io.to_slice)
      user.should eq "guest"
      pass.should eq "guest"
    end
  end

  describe "message codec" do
    it "round-trips body and core properties" do
      props = AMQ::Protocol::Properties.new(content_type: "text/plain", message_id: "m-1")
      encoded = MessageCodec.encode(props, "hello world".to_slice)
      parsed = MessageCodec.parse(encoded)
      String.new(parsed.body).should eq "hello world"
      parsed.properties.content_type.should eq "text/plain"
      parsed.properties.message_id.should eq "m-1"
    end

    it "maps durable header to delivery_mode" do
      io = IO::Memory.new
      # header with durable=true
      fl = FieldList.new
      fl.bool true
      Codec.write_described_list(io, Descriptor::HEADER, fl.fields)
      Codec.write(io, Described.new(Descriptor::DATA, "x".to_slice.as(Codec::AnyValue)))
      parsed = MessageCodec.parse(io.to_slice)
      parsed.properties.delivery_mode.should eq 2_u8
      String.new(parsed.body).should eq "x"
    end
  end
end
