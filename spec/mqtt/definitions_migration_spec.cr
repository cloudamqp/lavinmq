require "../spec_helper"
require "file_utils"

# Before MQTT got its own definitions file, a session was a durable queue of
# type "mqtt" and a subscription was a binding from the mqtt.default exchange.
# These helpers write a data dir in that shape so booting it exercises the
# migration.
private def legacy_vhost_dir(vhost : String) : String
  dir = Digest::SHA1.hexdigest(vhost)
  vhost_dir = File.join(LavinMQ::Config.instance.data_dir, dir)
  Dir.mkdir_p vhost_dir
  File.open(File.join(LavinMQ::Config.instance.data_dir, "vhosts.json"), "w") do |f|
    [{name: vhost, dir: dir}].to_json(f)
  end
  vhost_dir
end

private def write_legacy_definitions(vhost_dir : String, &)
  File.open(File.join(vhost_dir, "definitions.amqp"), "w") do |f|
    LavinMQ::SchemaVersion.prefix(f, :definition)
    yield f
  end
end

private def session_declare_frame(name : String)
  LavinMQ::AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, name, false, true, false, false, false,
    LavinMQ::AMQP::Table.new({"x-queue-type" => "mqtt"}))
end

private def subscription_bind_frame(name : String, topic_filter : String, qos : UInt8)
  LavinMQ::AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, name, LavinMQ::MQTT::EXCHANGE,
    topic_filter, false, LavinMQ::MQTT.qos_arguments(qos))
end

private def amqp_frames(vhost_dir : String) : Array(AMQ::Protocol::Frame)
  frames = Array(AMQ::Protocol::Frame).new
  File.open(File.join(vhost_dir, "definitions.amqp")) do |f|
    LavinMQ::SchemaVersion.verify(f, :definition)
    stream = AMQ::Protocol::Stream.new(f, format: IO::ByteFormat::SystemEndian)
    loop do
      frames << stream.next_frame
    rescue IO::EOFError
      break
    end
  end
  frames
end

private def mqtt_definitions_size(vhost_dir : String) : Int64
  File.size(File.join(vhost_dir, "definitions.mqtt"))
end

describe "MQTT definitions migration" do
  it "moves sessions and subscriptions out of definitions.amqp, keeping AMQP ones" do
    vhost_dir = legacy_vhost_dir("mqttmig")
    write_legacy_definitions(vhost_dir) do |f|
      f.write_bytes LavinMQ::AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, "keepx", "topic",
        false, true, false, false, false, LavinMQ::AMQP::Table.new)
      f.write_bytes LavinMQ::AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, "keepq", false, true,
        false, false, false, LavinMQ::AMQP::Table.new)
      f.write_bytes LavinMQ::AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, "keepq", "keepx", "rk",
        false, LavinMQ::AMQP::Table.new)
      f.write_bytes session_declare_frame("mqtt.migrated")
      f.write_bytes subscription_bind_frame("mqtt.migrated", "a/b", 1u8)
      f.write_bytes subscription_bind_frame("mqtt.migrated", "c/#", 0u8)
    end

    with_amqp_server do |s|
      v = s.vhosts["mqttmig"]

      session = v.session?("mqtt.migrated")
      session = session.should_not be_nil
      subscriptions = v.session_subscriptions(session)
      subscriptions.map(&.routing_key).sort!.should eq ["a/b", "c/#"]
      subscriptions.find! { |sub| sub.routing_key == "a/b" }.binding_key.qos.should eq 1u8
      subscriptions.find! { |sub| sub.routing_key == "c/#" }.binding_key.qos.should eq 0u8

      # The AMQP definitions are untouched by the migration
      v.exchange?("keepx").should_not be_nil
      v.queue?("keepq").should_not be_nil

      # definitions.amqp has been rewritten without anything MQTT
      frames = amqp_frames(vhost_dir)
      frames.should_not be_empty
      frames.each do |frame|
        case frame
        when LavinMQ::AMQP::Frame::Queue::Declare
          frame.arguments["x-queue-type"]?.should be_nil
        when LavinMQ::AMQP::Frame::Queue::Bind
          frame.exchange_name.should_not eq LavinMQ::MQTT::EXCHANGE
        end
      end

      # ... and definitions.mqtt carries them instead
      mqtt_definitions_size(vhost_dir).should be > 4

      # definitions.mqtt is authoritative from here on: the state survives a
      # restart even though definitions.amqp no longer mentions it
      restart_server(s)
      v = s.vhosts["mqttmig"]
      reloaded = v.session?("mqtt.migrated")
      reloaded = reloaded.should_not be_nil
      v.session_subscriptions(reloaded).map(&.routing_key).sort!.should eq ["a/b", "c/#"]
      v.exchange?("keepx").should_not be_nil
      v.queue?("keepq").should_not be_nil
    end
  end

  # A crash between "definitions.mqtt fsynced" and "definitions.amqp rewritten"
  # leaves the state in both files. The next boot has to read that as one set of
  # definitions, not two.
  it "is idempotent if definitions.amqp still holds the frames on the next boot" do
    vhost_dir = legacy_vhost_dir("mqttmig")
    write_legacy_definitions(vhost_dir) do |f|
      f.write_bytes session_declare_frame("mqtt.migrated")
      f.write_bytes subscription_bind_frame("mqtt.migrated", "a/b", 1u8)
    end
    legacy = File.join(vhost_dir, "legacy.amqp")
    FileUtils.cp File.join(vhost_dir, "definitions.amqp"), legacy

    with_amqp_server do |s|
      s.vhosts["mqttmig"].sessions_size.should eq 1

      # Put the pre-migration file back, as a crash mid-migration would leave it
      FileUtils.cp legacy, File.join(vhost_dir, "definitions.amqp")
      restart_server(s)

      v = s.vhosts["mqttmig"]
      v.sessions_size.should eq 1
      session = v.session("mqtt.migrated")
      subscriptions = v.session_subscriptions(session)
      subscriptions.map(&.routing_key).should eq ["a/b"]
      subscriptions.first.binding_key.qos.should eq 1u8
    end
  end

  # definitions.mqtt is the newer of the two: anything left in definitions.amqp
  # is by definition pre-migration. If a leftover frame were replayed over the
  # loaded state, a subscription's QoS would silently revert.
  it "does not let a leftover definitions.amqp frame override definitions.mqtt" do
    vhost_dir = legacy_vhost_dir("mqttmig")
    write_legacy_definitions(vhost_dir) do |f|
      f.write_bytes session_declare_frame("mqtt.migrated")
      f.write_bytes subscription_bind_frame("mqtt.migrated", "a/b", 0u8)
    end
    legacy = File.join(vhost_dir, "legacy.amqp")
    FileUtils.cp File.join(vhost_dir, "definitions.amqp"), legacy

    with_amqp_server do |s|
      v = s.vhosts["mqttmig"]
      session = v.session("mqtt.migrated")
      v.session_subscriptions(session).first.binding_key.qos.should eq 0u8

      # The client raises the QoS after the migration, so definitions.mqtt now
      # holds a newer value than the frame did
      v.mqtt.subscribe(session, "a/b", 1u8).should be_true

      # A crash mid-migration would have left the old frame in place
      FileUtils.cp legacy, File.join(vhost_dir, "definitions.amqp")
      restart_server(s)

      v = s.vhosts["mqttmig"]
      subscriptions = v.session_subscriptions(v.session("mqtt.migrated"))
      subscriptions.map(&.routing_key).should eq ["a/b"]
      subscriptions.first.binding_key.qos.should eq 1u8
    end
  end

  it "writes MQTT definitions only to definitions.mqtt" do
    with_amqp_server do |s|
      v = s.vhosts["/"]
      amqp_before = File.size(File.join(v.data_dir, "definitions.amqp"))
      mqtt_before = mqtt_definitions_size(v.data_dir)

      session = v.mqtt.declare_session("mqtt.sub", false)
      session = session.should_not be_nil
      v.mqtt.subscribe(session, "a/b", 1u8).should be_true

      File.size(File.join(v.data_dir, "definitions.amqp")).should eq amqp_before
      mqtt_definitions_size(v.data_dir).should be > mqtt_before
    end
  end

  it "compacts definitions.mqtt and reloads the surviving state" do
    with_amqp_server do |s|
      LavinMQ::Config.instance.max_deleted_definitions = 4
      v = s.vhosts["/"]
      keep = v.mqtt.declare_session("mqtt.keep", false)
      keep = keep.should_not be_nil
      v.mqtt.subscribe(keep, "a/b", 1u8)

      # Enough deletes to trip compaction
      LavinMQ::Config.instance.max_deleted_definitions.times do |i|
        churn = v.mqtt.declare_session("mqtt.churn#{i}", false)
        churn.should_not be_nil
        churn.try &.delete
      end

      restart_server(s)

      v = s.vhosts["/"]
      v.sessions_size.should eq 1
      v.session?("mqtt.keep").should_not be_nil
      subscriptions = v.session_subscriptions(v.session("mqtt.keep"))
      subscriptions.map(&.routing_key).should eq ["a/b"]
      subscriptions.first.binding_key.qos.should eq 1u8
    end
  end
end
