require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "publish" do
    it "should put the message in a queue" do
      with_server do |server|
        with_channel(server) do |ch|
          x = ch.exchange("mqtt", "direct")
          q = ch.queue("test")
          q.bind(x.name, q.name)

          with_client_io(server) do |io|
            connect(io)

            payload = slice = Bytes[1, 254, 200, 197, 123, 4, 87]
            ack = publish(io, topic: "test", payload: payload, qos: 1u8)
            ack.should be_a(MQTT::Protocol::PubAck)

            body = q.get(no_ack: true).try do |v|
              s = Slice(UInt8).new(payload.size)
              v.body_io.read(s)
              s
            end
            body.should eq(payload)
            disconnect(io)
          end
        end
      end
    end
  end
end
