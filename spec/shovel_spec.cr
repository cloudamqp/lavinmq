require "./spec_helper"
require "../src/avalanchemq/shovel"
require "http/server"

module ShovelSpecHelpers
  def self.setup_qs(ch, prefix = "") : {AMQP::Client::Exchange, AMQP::Client::Queue}
    x = ch.exchange("", "direct", passive: true)
    ch.queue("#{prefix}q1")
    q2 = ch.queue("#{prefix}q2")
    {x, q2}
  end

  def self.cleanup(prefix = "")
    s.vhosts["/"].delete_queue("#{prefix}q1")
    s.vhosts["/"].delete_queue("#{prefix}q2")
  end
end

describe AvalancheMQ::Shovel do
  describe "AMQP" do
    vhost = s.vhosts.create("x")

    it "should shovel and stop when queue length is met" do
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "ql_q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
      )
      dest = AvalancheMQ::Shovel::AMQPDestination.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "ql_q2",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
      )
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "ql_shovel", vhost)
      with_channel do |ch|
        x, q2 = ShovelSpecHelpers.setup_qs ch, "ql_"
        x.publish "shovel me 1", "ql_q1"
        x.publish "shovel me 2", "ql_q1"
        shovel.run
        sleep 10.milliseconds
        x.publish "shovel me 3", "ql_q1"
        sleep 10.milliseconds
        q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 1"
        q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 2"
        q2.get(no_ack: true).try(&.body_io.to_s).should be_nil
        s.vhosts["/"].shovels.not_nil!.empty?.should be_true
      end
    ensure
      ShovelSpecHelpers.cleanup "ql_"
      shovel.try &.delete
    end

    it "should shovel large messages" do
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "lm_q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
      )
      dest = AvalancheMQ::Shovel::AMQPDestination.new("spec", URI.parse(AMQP_BASE_URL), "lm_q2")
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "lm_shovel", vhost)
      with_channel do |ch|
        x, q2 = ShovelSpecHelpers.setup_qs ch, "lm_"
        x.publish_confirm "a" * 200_000, "lm_q1"
        shovel.run
        sleep 10.milliseconds
        q2.get(no_ack: true).not_nil!.body_io.to_s.bytesize.should eq 200_000
      end
    ensure
      ShovelSpecHelpers.cleanup "lm_"
      shovel.try &.delete
    end

    it "should shovel forever" do
      source = AvalancheMQ::Shovel::AMQPSource.new("spec", URI.parse(AMQP_BASE_URL), "sf_q1")
      dest = AvalancheMQ::Shovel::AMQPDestination.new("spec", URI.parse(AMQP_BASE_URL), "sf_q2")
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "sf_shovel", vhost)
      with_channel do |ch|
        x, q2 = ShovelSpecHelpers.setup_qs ch, "sf_"
        x.publish "shovel me 1", "sf_q1"
        x.publish "shovel me 2", "sf_q1"
        spawn { shovel.not_nil!.run }
        wait_for { shovel.not_nil!.running? }
        x.publish_confirm "shovel me 3", "sf_q1"
        q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 1"
        q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 2"
        q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 3"
        shovel.not_nil!.running?.should be_true
      end
    ensure
      ShovelSpecHelpers.cleanup "sf_"
      shovel.not_nil!.delete
    end

    it "should shovel with ack mode on-publish" do
      ack_mode = AvalancheMQ::Shovel::AckMode::OnPublish
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "ap_q1",
        prefetch: 1_u16,
        ack_mode: ack_mode
      )
      dest = AvalancheMQ::Shovel::AMQPDestination.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "ap_q2",
        ack_mode: ack_mode
      )
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "ap_shovel", vhost)
      with_channel do |ch|
        x, q2 = ShovelSpecHelpers.setup_qs ch, "ap_"
        x.publish "shovel me", "ap_q1"
        spawn { shovel.not_nil!.run }
        wait_for { shovel.not_nil!.running? }
        sleep 0.1 # Give time for message to be shoveled
        s.vhosts["/"].queues["ap_q1"].message_count.should eq 0
        q2.get(no_ack: false).try(&.body_io.to_s).should eq "shovel me"
      end
    ensure
      ShovelSpecHelpers.cleanup "ap_"
      shovel.not_nil!.delete
    end

    it "should shovel with ack mode no-ack" do
      ack_mode = AvalancheMQ::Shovel::AckMode::NoAck
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "na_q1",
        ack_mode: ack_mode
      )
      dest = AvalancheMQ::Shovel::AMQPDestination.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "na_q2",
        ack_mode: ack_mode
      )
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "na_shovel", vhost)
      with_channel do |ch|
        x, q2 = ShovelSpecHelpers.setup_qs ch, "na_"
        x.publish "shovel me", "na_q1"
        spawn { shovel.not_nil!.run }
        wait_for { shovel.not_nil!.running? }
        sleep 0.1 # Give time for message to be shoveled
        s.vhosts["/"].queues["na_q1"].message_count.should eq 0
        q2.get(no_ack: false).try(&.body_io.to_s).should eq "shovel me"
      end
    ensure
      ShovelSpecHelpers.cleanup "na_"
      shovel.not_nil!.delete
    end

    it "should shovel past prefetch" do
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "prefetch_q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength,
        prefetch: 21_u16
      )
      dest = AvalancheMQ::Shovel::AMQPDestination.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "prefetch_q2",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength,
        prefetch: 21_u16
      )
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "prefetch_shovel", vhost)
      with_channel do |ch|
        x = ShovelSpecHelpers.setup_qs(ch, "prefetch_").first
        100.times do
          x.publish "shovel me", "prefetch_q1"
        end
        wait_for { s.vhosts["/"].queues["prefetch_q1"].message_count == 100 }
        shovel.run
        wait_for { shovel.terminated? }
        s.vhosts["/"].queues["prefetch_q1"].message_count.should eq 0
        s.vhosts["/"].queues["prefetch_q2"].message_count.should eq 100
      end
    ensure
      ShovelSpecHelpers.cleanup("prefetch_")
      shovel.try &.delete
    end

    it "should shovel once qs are declared" do
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "od_q1"
      )
      dest = AvalancheMQ::Shovel::AMQPDestination.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "od_q2"
      )
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "od_shovel", vhost)
      with_channel do |ch|
        spawn { shovel.not_nil!.run }
        x, q2 = ShovelSpecHelpers.setup_qs ch, "od_"
        x.publish "shovel me", "od_q1"
        rmsg = nil
        wait_for { rmsg = q2.get(no_ack: true) }
        rmsg.not_nil!.body_io.to_s.should eq "shovel me"
      end
    ensure
      ShovelSpecHelpers.cleanup "od_"
      shovel.not_nil!.delete
    end

    it "should reconnect and continue" do
      config = %({
        "src-uri": "#{AMQP_BASE_URL}",
        "src-queue": "rc_q1",
        "dest-uri": "#{AMQP_BASE_URL}",
        "dest-queue": "rc_q2",
        "src-prefetch-count": 2})
      p = AvalancheMQ::Parameter.new("shovel", "rc_shovel", JSON.parse(config))
      s.vhosts["/"].add_parameter(p)
      with_channel do |ch|
        q1 = ch.queue("rc_q1", durable: true)
        ch.queue("rc_q2", durable: true)
        props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)
        q1.publish_confirm "shovel me 1", props: props
      end
      close_servers
      TestHelpers.setup
      wait_for { s.vhosts["/"].shovels.not_nil!["rc_shovel"]?.try(&.running?) }
      with_channel do |ch|
        q1 = ch.queue("rc_q1", durable: true)
        q2 = ch.queue("rc_q2", durable: true)
        props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)
        q1.publish_confirm "shovel me 2", props: props
        q1.publish_confirm "shovel me 3", props: props
        q1.publish_confirm "shovel me 4", props: props
        4.times do |i|
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me #{i + 1}"
        end
        s.vhosts["/"].queues["rc_q1"].message_count.should eq 0
      end
    ensure
      s.vhosts["/"].delete_queue("rc_q1")
      s.vhosts["/"].delete_queue("rc_q2")
      s.vhosts["/"].delete_parameter("shovel", "rc_shovel")
    end

    it "should shovel over amqps" do
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse("#{AMQPS_BASE_URL}?verify=none"),
        "ssl_q1"
      )
      dest = AvalancheMQ::Shovel::AMQPDestination.new(
        "spec",
        URI.parse("#{AMQPS_BASE_URL}?verify=none"),
        "ssl_q2"
      )
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "ssl_shovel", vhost)
      with_channel do |ch|
        x, q2 = ShovelSpecHelpers.setup_qs ch, "ssl_"
        spawn { shovel.not_nil!.run }
        x.publish "shovel me", "ssl_q1"
        msgs = Channel(AMQP::Client::Message).new
        q2.subscribe { |m| msgs.send m }
        msg = msgs.receive
        msg.body_io.to_s.should eq "shovel me"
      end
    ensure
      ShovelSpecHelpers.cleanup "ssl_"
      shovel.not_nil!.delete
    end

    it "should ack all messages that has been moved" do
      prefetch = 9_u16
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "prefetch2_q1",
        prefetch: prefetch
      )
      dest = AvalancheMQ::Shovel::AMQPDestination.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "prefetch2_q2",
        prefetch: prefetch
      )
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "prefetch2_shovel", vhost)
      with_channel do |ch|
        x = ShovelSpecHelpers.setup_qs(ch, "prefetch2_").first
        spawn { shovel.not_nil!.run }
        x.publish_confirm "shovel me 1", "prefetch2_q1"
        x.publish_confirm "shovel me 2", "prefetch2_q1"
        x.publish_confirm "shovel me 2", "prefetch2_q1"
        x.publish_confirm "shovel me 2", "prefetch2_q1"
        wait_for { s.vhosts["/"].queues["prefetch2_q2"].message_count == 4 }
        sleep 10.milliseconds
        shovel.not_nil!.terminate
        s.vhosts["/"].queues["prefetch2_q2"].message_count.should eq 4
        s.vhosts["/"].queues["prefetch2_q1"].message_count.should eq 0
      end
    ensure
      ShovelSpecHelpers.cleanup("prefetch2_")
      shovel.not_nil!.delete
    end

    describe "authentication error" do
      it "should be stopped" do
        source = AvalancheMQ::Shovel::AMQPSource.new(
          "spec",
          URI.parse("amqp://foo:bar@localhost:5672"),
          "q1"
        )
        dest = AvalancheMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse("amqp://foo:bar@localhost:5672"),
          "q2"
        )
        shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "auth_fail", vhost)
        spawn { shovel.run }
        wait_for { shovel.details_tuple[:error] }
        shovel.details_tuple[:error].should eq "403 - ACCESS_REFUSED"
        shovel.terminate
        shovel.state.should eq "Terminated"
      end
    end

    it "should count messages shoveled" do
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "c_q1"
      )
      dest = AvalancheMQ::Shovel::AMQPDestination.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "c_q2"
      )
      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "c_shovel", vhost)
      with_channel do |ch|
        x, _ = ShovelSpecHelpers.setup_qs ch, "c_"
        spawn { shovel.not_nil!.run }
        10.times do
          x.publish_confirm "shovel me", "c_q1"
        end
        wait_for { s.vhosts["/"].queues["c_q2"].message_count == 10 }
        shovel.not_nil!.details_tuple[:message_count].should eq 10
      end
      shovel.not_nil!.state.should eq "Running"
    ensure
      ShovelSpecHelpers.cleanup "c_"
      shovel.try &.delete
    end
  end

  describe "HTTP" do
    it "should shovel" do
      # # Setup HTTP server
      http_port = 16778
      h = Hash(String, String).new
      body = "<no body>"
      path = "<no path>"
      server = HTTP::Server.new do |context|
        context.request.headers.each do |k, v|
          h[k] = v.first
        end
        body = context.request.body.try &.gets
        path = context.request.path
        context.response.content_type = "text/plain"
        context.response.print "ok"
        context
      end
      server.bind_tcp http_port
      spawn { server.not_nil!.listen }

      vhost = s.vhosts.create("x")
      # # Setup shovel source and destination
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "ql_q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
      )
      dest = AvalancheMQ::Shovel::HTTPDestination.new(
        "spec",
        URI.parse("http://a:b@127.0.0.1:#{http_port}/pp")
      )

      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "ql_shovel", vhost)
      with_channel do |ch|
        x, _ = ShovelSpecHelpers.setup_qs ch, "ql_"
        headers = AMQP::Client::Arguments.new
        headers["a"] = "b"
        props = AMQP::Client::Properties.new("text/plain", nil, headers)
        x.publish "shovel me", "ql_q1", props: props
        shovel.run
        wait_for { shovel.running? || shovel.terminated? }

        # Check that we have sent one message successfully
        path.should eq "/pp"
        h["User-Agent"].should eq "AvalancheMQ"
        h["Content-Type"].should eq "text/plain"
        h["Authorization"].should eq "Basic YTpi" # base64 encoded "a:b"
        h["X-a"].should eq "b"
        body.should eq "shovel me"

        s.vhosts["/"].shovels.not_nil!.empty?.should be_true
      end
    ensure
      ShovelSpecHelpers.cleanup "ql_"
      shovel.try &.delete
      server.try &.close
    end

    it "should set path for URI from headers" do
      # # Setup HTTP server
      http_port = 16778
      path = "<no path>"
      server = HTTP::Server.new do |context|
        path = context.request.path
        context.response.content_type = "text/plain"
        context.response.print "ok"
        context
      end
      server.bind_tcp http_port
      spawn { server.not_nil!.listen }

      vhost = s.vhosts.create("x")
      # # Setup shovel source and destination
      source = AvalancheMQ::Shovel::AMQPSource.new(
        "spec",
        URI.parse(AMQP_BASE_URL),
        "ql_q1",
        delete_after: AvalancheMQ::Shovel::DeleteAfter::QueueLength
      )
      dest = AvalancheMQ::Shovel::HTTPDestination.new(
        "spec",
        URI.parse("http://a:b@127.0.0.1:#{http_port}")
      )

      shovel = AvalancheMQ::Shovel::Runner.new(source, dest, "ql_shovel", vhost)
      with_channel do |ch|
        x, _ = ShovelSpecHelpers.setup_qs ch, "ql_"
        headers = AMQP::Client::Arguments.new
        headers["uri_path"] = "/some_path"
        props = AMQP::Client::Properties.new("text/plain", nil, headers)
        x.publish_confirm "shovel me", "ql_q1", props: props
        shovel.run
        sleep 10.milliseconds # better when than sleep?
        path.should eq "/some_path"
      end
    ensure
      ShovelSpecHelpers.cleanup "ql_"
      shovel.try &.delete
      server.try &.close
    end
  end
end
