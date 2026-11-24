require "./spec_helper"

describe "Manual exchange" do
  x_name = "mailroom"
  pending_q = "amq.manual-#{x_name}"
  x_args = AMQP::Client::Arguments.new({"x-manual-exchange" => true, "alternate-exchange" => "unroutable"})

  # A direct manual exchange with the bindings toy → workshop and cookie →
  # kitchen, and an alternate exchange that leads to the queue fire
  setup = ->(ch : AMQP::Client::Channel) do
    ch.exchange("unroutable", "fanout")
    ch.queue("fire").bind("unroutable", "")
    ch.exchange(x_name, "direct", args: x_args)
    ch.queue("workshop").bind(x_name, "toy")
    ch.queue("kitchen").bind(x_name, "cookie")
    ch.confirm_select
  end

  decide = ->(ch : AMQP::Client::Channel, tag : UInt64, route : String) do
    hdrs = AMQP::Client::Arguments.new({"x-manual-delivery-tag" => tag.to_i64, "x-manual-route" => route})
    ch.basic_publish_confirm "", x_name, props: AMQP::Client::Properties.new(headers: hdrs)
  end

  it "parks published messages in its pending queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        ch.basic_publish_confirm "letter", x_name, "toy"
        s.vhosts["/"].queue(pending_q).message_count.should eq 1
        s.vhosts["/"].queue("workshop").message_count.should eq 0
      end
    end
  end

  it "routes the stored message to the queue that the decision names" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        ch.basic_publish_confirm "letter", x_name, "toy"
        msg = ch.basic_get(pending_q, no_ack: false).should_not be_nil
        decide.call(ch, msg.delivery_tag, "workshop").should be_true

        s.vhosts["/"].queue(pending_q).message_count.should eq 0
        s.vhosts["/"].queue(pending_q).unacked_count.should eq 0
        routed = ch.basic_get("workshop", no_ack: true).should_not be_nil
        routed.body_io.to_s.should eq "letter"
        routed.routing_key.should eq "toy"
        hdrs = routed.properties.headers.should_not be_nil
        hdrs["x-manual-route"].should eq "workshop"
        hdrs["x-manual-match"].should be_true
        hdrs["x-manual-routed-by"].as(String).should_not be_empty
      end
    end
  end

  it "routes a wrong decision and marks it as no match" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        ch.basic_publish_confirm "letter", x_name, "toy"
        msg = ch.basic_get(pending_q, no_ack: false).should_not be_nil
        decide.call(ch, msg.delivery_tag, "kitchen").should be_true
        routed = ch.basic_get("kitchen", no_ack: true).should_not be_nil
        (routed.properties.headers.should_not be_nil)["x-manual-match"].should be_false
      end
    end
  end

  it "sends the decision none to the alternate exchange" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        ch.basic_publish_confirm "coal", x_name, "coal"
        ch.basic_publish_confirm "toy", x_name, "toy"
        2.times do
          msg = ch.basic_get(pending_q, no_ack: false).should_not be_nil
          decide.call(ch, msg.delivery_tag, "none").should be_true
        end
        coal = ch.basic_get("fire", no_ack: true).should_not be_nil
        (coal.properties.headers.should_not be_nil)["x-manual-match"].should be_true
        toy = ch.basic_get("fire", no_ack: true).should_not be_nil
        (toy.properties.headers.should_not be_nil)["x-manual-match"].should be_false
        s.vhosts["/"].queue("workshop").message_count.should eq 0
      end
    end
  end

  it "handles a queue that no binding leads to as unroutable" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        ch.queue("elsewhere")
        ch.basic_publish_confirm "letter", x_name, "toy"
        msg = ch.basic_get(pending_q, no_ack: false).should_not be_nil
        decide.call(ch, msg.delivery_tag, "elsewhere").should be_true
        s.vhosts["/"].queue("elsewhere").message_count.should eq 0
        s.vhosts["/"].queue("fire").message_count.should eq 1
        s.vhosts["/"].queue(pending_q).unacked_count.should eq 0
      end
    end
  end

  it "routes with its own bindings when the decision is auto" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        ch.basic_publish_confirm "letter", x_name, "cookie"
        msg = ch.basic_get(pending_q, no_ack: false).should_not be_nil
        decide.call(ch, msg.delivery_tag, "auto").should be_true
        routed = ch.basic_get("kitchen", no_ack: true).should_not be_nil
        (routed.properties.headers.should_not be_nil)["x-manual-match"].should be_true
      end
    end
  end

  it "closes the channel on an unknown delivery tag and requeues what it held" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        ch.basic_publish_confirm "letter", x_name, "toy"
        msg = ch.basic_get(pending_q, no_ack: false).should_not be_nil
        expect_raises(AMQP::Client::Channel::ClosedException, /unknown delivery tag/) do
          decide.call(ch, msg.delivery_tag + 10, "workshop")
        end
      end
      wait_for { s.vhosts["/"].queue(pending_q).message_count == 1 }
      s.vhosts["/"].queue("workshop").message_count.should eq 0
    end
  end

  it "closes the channel on a second decision for the same tag" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        ch.basic_publish_confirm "letter", x_name, "toy"
        msg = ch.basic_get(pending_q, no_ack: false).should_not be_nil
        decide.call(ch, msg.delivery_tag, "workshop").should be_true
        expect_raises(AMQP::Client::Channel::ClosedException, /unknown delivery tag/) do
          decide.call(ch, msg.delivery_tag, "kitchen")
        end
      end
      s.vhosts["/"].queue("workshop").message_count.should eq 1
      s.vhosts["/"].queue("kitchen").message_count.should eq 0
    end
  end

  it "removes decision headers from a normal publish" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        hdrs = AMQP::Client::Arguments.new({"x-manual-route" => "workshop", "x-manual-match" => true, "other" => "kept"})
        ch.basic_publish_confirm "letter", x_name, "toy", props: AMQP::Client::Properties.new(headers: hdrs)
        msg = ch.basic_get(pending_q, no_ack: true).should_not be_nil
        h = msg.properties.headers.should_not be_nil
        h["x-manual-route"]?.should be_nil
        h["x-manual-match"]?.should be_nil
        h["other"].should eq "kept"
      end
    end
  end

  it "nacks a publish when the pending queue is full" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        args = AMQP::Client::Arguments.new({"x-manual-exchange" => true, "x-manual-max-length" => 1})
        ch.exchange("small", "direct", args: args)
        ch.confirm_select
        ch.basic_publish_confirm("one", "small", "toy").should be_true
        ch.basic_publish_confirm("two", "small", "toy").should be_false
      end
    end
  end

  it "drops a publish to the pending queue through the default exchange" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        ch.basic_publish_confirm "sneaky", "", pending_q
        s.vhosts["/"].queue(pending_q).message_count.should eq 0
      end
    end
  end

  it "refuses an exchange that is both manual and delayed" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        args = AMQP::Client::Arguments.new({"x-manual-exchange" => true, "x-delayed-exchange" => true})
        expect_raises(AMQP::Client::Channel::ClosedException, /manual and delayed/) do
          ch.exchange("both", "direct", args: args)
        end
      end
    end
  end

  it "deletes its pending queue with the exchange" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        setup.call(ch)
        s.vhosts["/"].queue?(pending_q).should_not be_nil
        ch.exchange_delete(x_name)
        s.vhosts["/"].queue?(pending_q).should be_nil
      end
    end
  end

  describe "permissions" do
    it "lets a router with only read on the pending queue decide, but not publish" do
      with_amqp_server do |s|
        s.users.create("router", "pw")
        s.users.add_permission("router", "/", /^$/, /^amq\.manual-mailroom$/, /^$/)
        with_channel(s) do |ch|
          setup.call(ch)
          ch.basic_publish_confirm "letter", x_name, "toy"
        end
        with_channel(s, user: "router", password: "pw") do |ch|
          ch.confirm_select
          msg = ch.basic_get(pending_q, no_ack: false).should_not be_nil
          decide.call(ch, msg.delivery_tag, "workshop").should be_true
          expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
            ch.basic_publish_confirm "new letter", x_name, "toy"
          end
        end
        s.vhosts["/"].queue("workshop").message_count.should eq 1
        s.vhosts["/"].queue(pending_q).message_count.should eq 0
      end
    end

    it "lets a producer publish but not read the pending queue" do
      with_amqp_server do |s|
        s.users.create("producer", "pw")
        s.users.add_permission("producer", "/", /^$/, /^$/, /^mailroom$/)
        with_channel(s) { |ch| setup.call(ch) }
        with_channel(s, user: "producer", password: "pw") do |ch|
          ch.confirm_select
          ch.basic_publish_confirm("letter", x_name, "toy").should be_true
          expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
            ch.basic_get(pending_q, no_ack: false)
          end
        end
      end
    end

    it "still refuses to bind or purge the pending queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          setup.call(ch)
          expect_raises(AMQP::Client::Channel::ClosedException, /internal queue/) do
            ch.queue_bind(pending_q, "unroutable", "")
          end
        end
      end
    end
  end
end
