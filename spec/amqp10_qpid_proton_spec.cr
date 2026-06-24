require "./spec_helper"
require "qpid-proton"

private def with_qpid_proton_client(server : LavinMQ::Server, &)
  Qpid::Proton::Client.open("localhost", amqp_port(server),
    username: "guest", password: "guest", allow_insecure_mechanisms: true) do |client|
    yield client
  end
end

private def qpid_proton_body(message : Qpid::Proton::Message) : String
  body = message.body
  body.rewind
  body.next.should be_true

  case body.type
  when Qpid::Proton::Lib::Type::String, Qpid::Proton::Lib::Type::Symbol
    body.string
  when Qpid::Proton::Lib::Type::Binary
    String.new(body.binary)
  else
    fail "unexpected Proton message body type #{body.type_name}"
  end
end

describe "AMQP 1.0 Qpid Proton compatibility" do
  it "publishes Proton messages to LavinMQ queues" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("qpid-proton-publish", auto_delete: true)

        with_qpid_proton_client(s) do |client|
          client.publish("/queues/#{q.name}", "hello from proton")
        end

        msg = q.get(no_ack: true).not_nil!
        msg.body_io.gets_to_end.should eq "hello from proton"
      end
    end
  end

  it "consumes LavinMQ queue messages with Proton receivers" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("qpid-proton-consume", durable: false)
        q.publish("hello to proton")

        with_qpid_proton_client(s) do |client|
          message = client.receive("/queues/#{q.name}").not_nil!
          qpid_proton_body(message).should eq "hello to proton"
        end

        should_eventually(eq 0) { s.vhosts["/"].queue(q.name).message_count }
      end
    end
  end
end
