require "./spec_helper"
require "../src/lavinmq/shovel"
require "http/server"
require "wait_group"

module ShovelSpecHelpers
  def self.setup_qs(ch, prefix = "") : {AMQP::Client::Exchange, AMQP::Client::Queue}
    x = ch.exchange("", "direct", passive: true)
    ch.queue("#{prefix}q1")
    q2 = ch.queue("#{prefix}q2")
    {x, q2}
  end
end

describe LavinMQ::Shovel do
  describe "AMQP" do
    describe "Source" do
      it "will stop and raise on unexpected disconnect" do
        with_amqp_server do |s|
          source = LavinMQ::Shovel::AMQPSource.new(
            "spec",
            [URI.parse(s.amqp_url)],
            "source",
            direct_user: s.users.direct_user
          )

          source.start

          s.vhosts["/"].queues["source"].publish(LavinMQ::Message.new("", "", ""))
          expect_raises(AMQP::Client::Connection::ClosedException) do
            source.each do
              s.vhosts["/"].connections.each &.close("spec")
            end
          end
          source.started?.should be_false
        end
      end

      it "will start ack timeout loop if needed" do
        with_amqp_server do |s|
          source_name = Random::Secure.base64(32)
          source = LavinMQ::Shovel::AMQPSource.new(
            source_name,
            [URI.parse(s.amqp_url)],
            "source",
            prefetch: 100,
            direct_user: s.users.direct_user
          )

          source.start

          s.vhosts["/"].queues["source"].publish(LavinMQ::Message.new("", "", ""))
          wg = WaitGroup.new(1)
          spawn { source.each { wg.done } }
          wg.wait

          fiber_found = false
          Fiber.list do |f|
            if (name = f.name) && name.includes?("ack timeout loop") && name.includes?(source_name)
              fiber_found = true
            end
          end
          source.stop

          fiber_found.should be_true
        end
      end

      it "wont start ack timeout loop when not needed" do
        with_amqp_server do |s|
          source_name = Random::Secure.base64(32)
          source = LavinMQ::Shovel::AMQPSource.new(
            source_name,
            [URI.parse(s.amqp_url)],
            "source",
            prefetch: 1,
            direct_user: s.users.direct_user
          )

          source.start

          s.vhosts["/"].queues["source"].publish(LavinMQ::Message.new("", "", ""))
          wg = WaitGroup.new(1)
          spawn { source.each { wg.done } }
          wg.wait

          fiber_found = false
          Fiber.list do |f|
            if (name = f.name) && name.includes?("ack timeout loop") && name.includes?(source_name)
              fiber_found = true
            end
          end
          source.stop

          fiber_found.should be_false
        end
      end

      it "will ack after timeout" do
        with_amqp_server do |s|
          source_name = Random::Secure.base64(32)
          source = LavinMQ::Shovel::AMQPSource.new(
            source_name,
            [URI.parse(s.amqp_url)],
            "source",
            prefetch: 10,
            direct_user: s.users.direct_user,
            batch_ack_timeout: 1.nanosecond
          )

          source.start

          s.vhosts["/"].queues["source"].publish(LavinMQ::Message.new("", "", ""))
          wg = WaitGroup.new(1)
          spawn { source.each { |m| source.ack(m.delivery_tag) && wg.done } }
          wg.wait
          sleep 1.millisecond
          s.vhosts["/"].queues["source"].unacked_count.should eq 0
          source.stop
        end
      end
    end

    it "will wait to ack all msgs before deleting it self" do
      with_amqp_server do |s|
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "d",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_url),
          "q",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ql_shovel", s.vhosts["/"])
        with_channel(s) do |ch|
          q = ch.queue("q", args: AMQ::Protocol::Table.new({"x-dead-letter-exchange": "amq.fanout"}))
          d = ch.queue("d")
          d.bind("amq.fanout", "")
          done = WaitGroup.new
          q.subscribe(no_ack: false) { |msg| msg.reject; done.done }
          done.add
          q.publish "foobar"
          done.wait
          done.add
          shovel.run
          done.wait
          q.message_count.should eq 0
          d.message_count.should eq 1
        end
      end
    end

    it "should shovel and stop when queue length is met" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "ql_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_url),
          "ql_q2",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ql_shovel", vhost)
        with_channel(s) do |ch|
          x, q2 = ShovelSpecHelpers.setup_qs ch, "ql_"
          x.publish_confirm "shovel me 1", "ql_q1"
          x.publish_confirm "shovel me 2", "ql_q1"
          shovel.run
          x.publish_confirm "shovel me 3", "ql_q1"
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 1"
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 2"
          q2.get(no_ack: true).try(&.body_io.to_s).should be_nil
          s.vhosts["/"].shovels.empty?.should be_true
        end
      end
    end

    it "should shovel large messages" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "lm_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new("spec", URI.parse(s.amqp_url), "lm_q2", direct_user: s.users.direct_user)
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "lm_shovel", vhost)
        with_channel(s) do |ch|
          x, q2 = ShovelSpecHelpers.setup_qs ch, "lm_"
          x.publish_confirm "a" * 200_000, "lm_q1"
          shovel.run
          sleep 10.milliseconds
          q2.get(no_ack: true).not_nil!.body_io.to_s.bytesize.should eq 200_000
        end
      end
    end

    it "should shovel forever" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new("spec", [URI.parse(s.amqp_url)], "sf_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::AMQPDestination.new("spec", URI.parse(s.amqp_url), "sf_q2", direct_user: s.users.direct_user)
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "sf_shovel", vhost)
        with_channel(s) do |ch|
          x, q2 = ShovelSpecHelpers.setup_qs ch, "sf_"
          x.publish_confirm "shovel me 1", "sf_q1"
          x.publish_confirm "shovel me 2", "sf_q1"
          spawn shovel.run
          msgs = Channel(String).new
          q2.subscribe(no_ack: true) do |msg|
            msgs.send(msg.body_io.to_s)
          end
          wait_for { shovel.running? }
          x.publish_confirm "shovel me 3", "sf_q1"
          3.times do |i|
            msgs.receive.should eq "shovel me #{i + 1}"
          end
          shovel.running?.should be_true
        end
      end
    end

    it "should shovel with ack mode on-publish" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        ack_mode = LavinMQ::Shovel::AckMode::OnPublish
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "ap_q1",
          prefetch: 1_u16,
          ack_mode: ack_mode,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_url),
          "ap_q2",
          ack_mode: ack_mode,
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ap_shovel", vhost)
        with_channel(s) do |ch|
          x, q2 = ShovelSpecHelpers.setup_qs ch, "ap_"
          x.publish_confirm "shovel me", "ap_q1"
          spawn shovel.run
          wait_for { shovel.running? }
          sleep 0.1.seconds # Give time for message to be shoveled
          s.vhosts["/"].queues["ap_q1"].message_count.should eq 0
          q2.get(no_ack: false).try(&.body_io.to_s).should eq "shovel me"
        end
      end
    end

    it "should shovel with ack mode no-ack" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        ack_mode = LavinMQ::Shovel::AckMode::NoAck
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "na_q1",
          ack_mode: ack_mode,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_url),
          "na_q2",
          ack_mode: ack_mode,
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "na_shovel", vhost)
        with_channel(s) do |ch|
          x, q2 = ShovelSpecHelpers.setup_qs ch, "na_"
          x.publish_confirm "shovel me", "na_q1"
          spawn { shovel.run }
          wait_for { s.vhosts["/"].queues["na_q1"].message_count.zero? }
          wait_for { !s.vhosts["/"].queues["na_q2"].message_count.zero? }
          q2.get(no_ack: false).try(&.body_io.to_s).should eq "shovel me"
        end
      end
    end

    it "should shovel past prefetch" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "prefetch_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          prefetch: 21_u16,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_url),
          "prefetch_q2",
          direct_user: s.users.direct_user
        )
        with_channel(s) do |ch|
          x = ShovelSpecHelpers.setup_qs(ch, "prefetch_").first
          100.times do
            x.publish_confirm "shovel me", "prefetch_q1"
          end
          wait_for { s.vhosts["/"].queues["prefetch_q1"].message_count == 100 }
          shovel = LavinMQ::Shovel::Runner.new(source, dest, "prefetch_shovel", vhost)
          shovel.run
          wait_for { shovel.terminated? }
          s.vhosts["/"].queues["prefetch_q1"].message_count.should eq 0
          s.vhosts["/"].queues["prefetch_q2"].message_count.should eq 100
        end
      end
    end

    it "should shovel once qs are declared" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "od_q1",
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_url),
          "od_q2",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "od_shovel", vhost)
        with_channel(s) do |ch|
          spawn { shovel.run }
          x, q2 = ShovelSpecHelpers.setup_qs ch, "od_"
          x.publish_confirm "shovel me", "od_q1"
          rmsg = nil
          wait_for { rmsg = q2.get(no_ack: true) }
          rmsg.not_nil!.body_io.to_s.should eq "shovel me"
        end
      end
    end

    it "should reconnect and continue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q1 = ch.queue("rc_q1")
          _q2 = ch.queue("rc_q2")
          q1.publish_confirm "shovel me 1", props: AMQ::Protocol::Properties.new(delivery_mode: 2_u8)
        end
        config = %({
        "src-uri": "#{s.amqp_url}",
        "src-queue": "rc_q1",
        "dest-uri": "#{s.amqp_url}",
        "dest-queue": "rc_q2",
        "src-prefetch-count": 2})
        p = LavinMQ::Parameter.new("shovel", "rc_shovel", JSON.parse(config))
        s.vhosts["/"].add_parameter(p)
        s.restart
        wait_for { s.vhosts["/"].shovels.size > 0 }
        shovel = s.vhosts["/"].shovels["rc_shovel"]
        wait_for { shovel.running? }
        with_channel(s) do |ch|
          q1 = ch.queue("rc_q1", durable: true)
          q2 = ch.queue("rc_q2", durable: true)
          msgs = Channel(String).new
          q2.subscribe(no_ack: true) do |msg|
            msgs.send(msg.body_io.to_s)
          end
          props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)
          spawn do
            q1.publish_confirm "shovel me 2", props: props
            q1.publish_confirm "shovel me 3", props: props
            q1.publish_confirm "shovel me 4", props: props
          end
          4.times do |i|
            msgs.receive.should eq "shovel me #{i + 1}"
          end
          ch.queue_declare("rc_q1", passive: true)[:message_count].should eq 0
        end
      end
    end

    it "should shovel over amqps" do
      with_amqp_server(tls: true) do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse("#{s.amqp_url.sub("amqp://", "amqps://")}?verify=none")],
          "ssl_q1",
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse("#{s.amqp_url.sub("amqp://", "amqps://")}?verify=none"),
          "ssl_q2",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ssl_shovel", vhost)
        with_channel(s, tls: true, verify_mode: OpenSSL::SSL::VerifyMode::NONE) do |ch|
          x, q2 = ShovelSpecHelpers.setup_qs ch, "ssl_"
          spawn { shovel.run }
          x.publish_confirm "shovel me", "ssl_q1"
          msgs = Channel(AMQP::Client::DeliverMessage).new
          q2.subscribe { |m| msgs.send m }
          msg = msgs.receive
          msg.body_io.to_s.should eq "shovel me"
        end
      end
    end

    it "should ack all messages that has been moved" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        # Use prefetch 5, then the batch ack will ack every third message
        prefetch = 5_u16
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "prefetch2_q1",
          prefetch: prefetch,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_url),
          "prefetch2_q2",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "prefetch2_shovel", vhost)
        with_channel(s) do |ch|
          x = ShovelSpecHelpers.setup_qs(ch, "prefetch2_").first
          spawn { shovel.run }
          x.publish_confirm "shovel me 1", "prefetch2_q1"
          x.publish_confirm "shovel me 2", "prefetch2_q1"
          x.publish_confirm "shovel me 2", "prefetch2_q1"
          x.publish_confirm "shovel me 2", "prefetch2_q1"
          # Wait until four messages has been published to destination...
          wait_for { s.vhosts["/"].queues["prefetch2_q2"].message_count == 4 }
          # ... but only three has been acked (because batching)
          wait_for { s.vhosts["/"].queues["prefetch2_q1"].unacked_count == 1 }
          # Now when we terminate the shovel it should ack the last message(s)
          shovel.terminate
          wait_for { s.vhosts["/"].queues["prefetch2_q1"].unacked_count == 0 }
          s.vhosts["/"].queues["prefetch2_q2"].message_count.should eq 4
          s.vhosts["/"].queues["prefetch2_q1"].message_count.should eq 0
        end
      end
    end

    describe "authentication error" do
      it "should be stopped" do
        with_amqp_server do |s|
          vhost = s.vhosts.create("x")
          uri = URI.parse(s.amqp_url)
          uri.user = "foo"
          uri.password = "bar"
          source = LavinMQ::Shovel::AMQPSource.new(
            "spec",
            [uri],
            "q1",
            direct_user: s.users.direct_user
          )
          dest = LavinMQ::Shovel::AMQPDestination.new(
            "spec",
            uri,
            "q2",
            direct_user: s.users.direct_user
          )
          shovel = LavinMQ::Shovel::Runner.new(source, dest, "auth_fail", vhost)
          spawn { shovel.run }
          wait_for { shovel.details_tuple[:error] }
          shovel.details_tuple[:error].not_nil!.should contain "ACCESS_REFUSED"
          shovel.terminate
          shovel.state.should eq "Terminated"
        end
      end
    end

    it "should count messages shoveled" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "c_q1",
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_url),
          "c_q2",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "c_shovel", vhost)
        with_channel(s) do |ch|
          x, _ = ShovelSpecHelpers.setup_qs ch, "c_"
          spawn { shovel.run }
          10.times do
            x.publish_confirm "shovel me", "c_q1"
          end
          wait_for { s.vhosts["/"].queues["c_q2"].message_count == 10 }
          shovel.details_tuple[:message_count].should eq 10
        end
        shovel.state.should eq "Running"
      end
    end

    it "should shovel stream queues" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        q1_name = "stream_q1"
        q2_name = "stream_q2"
        consumer_args = {"x-stream-offset" => JSON::Any.new("first")}
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          q1_name,
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user,
          consumer_args: consumer_args
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_url),
          q2_name,
          direct_user: s.users.direct_user,
        )

        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ql_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.prefetch 1
          args = AMQP::Client::Arguments.new({"x-queue-type" => "stream"})
          q1 = ch.queue(q1_name, args: args)
          q2 = ch.queue(q2_name, args: args)

          10.times do
            x.publish_confirm "shovel me", q1_name
          end
          shovel.run

          q1_msg_count = 0
          q2_msg_count = 0
          q1.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "first"})) do |msg|
            msg.ack
            q1_msg_count += 1
          end
          q2.subscribe(no_ack: false, args: AMQP::Client::Arguments.new({"x-stream-offset": "first"})) do |msg|
            msg.ack
            q2_msg_count += 1
          end

          should_eventually(be_true) { q1_msg_count == 10 }
          should_eventually(be_true) { q2_msg_count == 10 }
          s.vhosts["/"].shovels.empty?.should be_true
        end
      end
    end

    it "should move messages between queues with long names" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        long_prefix = "a" * 250
        source = LavinMQ::Shovel::AMQPSource.new(
          "#{long_prefix}q1",
          [URI.parse(s.amqp_url)],
          "#{long_prefix}q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "#{long_prefix}q2",
          URI.parse(s.amqp_url),
          "#{long_prefix}q2",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ql_shovel", vhost)
        with_channel(s) do |ch|
          q1, q2 = ShovelSpecHelpers.setup_qs ch, long_prefix
          q1.publish_confirm "shovel me 1", "#{long_prefix}q1"
          q1.publish_confirm "shovel me 2", "#{long_prefix}q1"
          shovel.run
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 1"
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 2"
          s.vhosts["/"].shovels.empty?.should be_true
        end
      end
    end

    it "should pause and resume shovel" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("pause:resume:vhost")
        queue_name = "shovel:pause:resume"
        source = LavinMQ::Shovel::AMQPSource.new(
          "#{queue_name}q1",
          [URI.parse(s.amqp_url)],
          "#{queue_name}q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "#{queue_name}q2",
          URI.parse(s.amqp_url),
          "#{queue_name}q2",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "pause:resume:shovel", vhost)
        with_channel(s) do |ch|
          q1, q2 = ShovelSpecHelpers.setup_qs ch, queue_name
          q1.publish_confirm "shovel me 1", "#{queue_name}q1"
          q1.publish_confirm "shovel me 2", "#{queue_name}q1"
          shovel.run
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 1"
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 2"
          shovel.pause
          shovel.paused?.should eq true

          q1.publish_confirm "shovel me 3", "#{queue_name}q1"
          q1.publish_confirm "shovel me 4", "#{queue_name}q1"
          q2.get(no_ack: true).try(&.body_io.to_s).should eq nil

          spawn shovel.resume
          wait_for { shovel.running? } # Ensure it gets back to Running state
          wait_for { shovel.terminated? }
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 3"
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 4"
          s.vhosts["/"].shovels.empty?.should be_true
        end
      end
    end
  end

  describe "HTTP" do
    it "should shovel" do
      with_amqp_server do |s|
        # # Setup HTTP server
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
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts.create("x")
        # # Setup shovel source and destination
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "ql_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::HTTPDestination.new(
          "spec",
          URI.parse("http://a:b@#{addr}/pp")
        )

        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ql_shovel", vhost)
        with_channel(s) do |ch|
          x, _ = ShovelSpecHelpers.setup_qs ch, "ql_"
          headers = AMQP::Client::Arguments.new
          headers["a"] = "b"
          props = AMQP::Client::Properties.new("text/plain", nil, headers)
          x.publish_confirm "shovel me", "ql_q1", props: props
          shovel.run
          sleep 10.milliseconds

          # Check that we have sent one message successfully
          path.should eq "/pp"
          h["User-Agent"].should eq "LavinMQ"
          h["Content-Type"].should eq "text/plain"
          h["Authorization"].should eq "Basic YTpi" # base64 encoded "a:b"
          h["X-a"].should eq "b"
          body.should eq "shovel me"

          s.vhosts["/"].shovels.empty?.should be_true
        end
      end
    end

    it "should set path for URI from headers" do
      with_amqp_server do |s|
        # # Setup HTTP server
        path = "<no path>"
        server = HTTP::Server.new do |context|
          path = context.request.path
          context.response.content_type = "text/plain"
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts.create("x")
        # # Setup shovel source and destination
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_url)],
          "ql_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::HTTPDestination.new(
          "spec",
          URI.parse("http://a:b@#{addr}")
        )

        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ql_shovel", vhost)
        with_channel(s) do |ch|
          x, _ = ShovelSpecHelpers.setup_qs ch, "ql_"
          headers = AMQP::Client::Arguments.new
          headers["uri_path"] = "/some_path"
          props = AMQP::Client::Properties.new("text/plain", nil, headers)
          x.publish_confirm "shovel me", "ql_q1", props: props
          shovel.run
          sleep 10.milliseconds # better when than sleep?
          path.should eq "/some_path"
        end
      end
    end
  end
end
