require "./spec_helper"

module CCandBCCHeaderSpecs
  class_property! ch : AMQP::Client::Channel
  class_property! q : AMQP::Client::Queue

  def self.with_q(&)
    with_amqp_server do |s|
      with_channel(s) do |channel|
        CCandBCCHeaderSpecs.ch = channel
        CCandBCCHeaderSpecs.q = channel.queue("q")
        CCandBCCHeaderSpecs.q.bind("amq.direct", "q")
        yield
      end
    end
  end

  def self.pub(payload, props : AMQP::Client::Properties? = nil)
    ch.direct_exchange.publish_confirm(payload, q.name, props: props)
  end

  def self.get
    q.get.should be_a AMQP::Client::GetMessage
  end

  describe "CC and BCC" do
    around_each do |example|
      with_q do
        example.run
      end
    end

    describe "CC header" do
      it "should be preserved" do
        props = AMQP::Client::Properties.new(
          headers: AMQ::Protocol::Table.new({CC: ["foo"]})
        )
        pub("msg1", props)
        msg = get
        cc = msg.properties.headers.try &.["CC"].should be_a Array(AMQ::Protocol::Field)
        cc.should eq ["foo"]
      end
    end

    describe "BCC header" do
      it "should be removed on delivery" do
        props = AMQP::Client::Properties.new(
          headers: AMQ::Protocol::Table.new({BCC: ["foo"]})
        )
        pub("msg1", props)
        msg = get
        msg.properties.headers.try(&.["BCC"]?).should be_nil
      end
    end
  end
end
