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

  class PauseRaceSource < LavinMQ::Shovel::Source
    @each_count = Atomic(UInt32).new(0_u32)
    @stopped = Channel(Bool).new(1)

    getter delete_after = LavinMQ::Shovel::DeleteAfter::Never
    getter first_each_entered = Channel(Bool).new(1)
    getter second_each_entered = Channel(Bool).new(1)
    getter release_first_each = Channel(Bool).new

    def start
      while @stopped.try_receive?
      end
    end

    def stop
      @stopped.try_send? true
    end

    def ack(delivery_tag, batch = true)
    end

    def reject(delivery_tag, requeue)
    end

    def each(&_blk : ::AMQP::Client::DeliverMessage -> Nil)
      case @each_count.add(1_u32, :relaxed)
      when 0
        @first_each_entered.send true
        @release_first_each.receive
      when 1
        @second_each_entered.send true
        @stopped.receive?
      end
    end
  end

  class PauseRaceDestination < LavinMQ::Shovel::Destination
    def start
    end

    def stop
    end

    def push(msg)
    end

    def started? : Bool
      true
    end
  end

  # A destination that starts cleanly and reports nothing on its own, so a test
  # can drive MultiDestinationHandler#report directly.
  class StubDestination < LavinMQ::Shovel::Destination
    def start
    end

    def stop
    end

    def push(msg)
    end

    def started? : Bool
      true
    end
  end

  # Records every Outcome a Destination reports, for testing it in isolation
  # from the Runner/Source.
  class RecordingListener
    include LavinMQ::Shovel::OutcomeListener
    getter outcomes = [] of {UInt64, LavinMQ::Shovel::Outcome}

    def report(delivery_tag : UInt64, outcome : LavinMQ::Shovel::Outcome)
      @outcomes << {delivery_tag, outcome}
    end
  end
end

describe LavinMQ::Shovel do
  describe "AMQP" do
    describe "Source" do
      it "will stop and raise on unexpected disconnect" do
        with_amqp_server do |s|
          source = LavinMQ::Shovel::AMQPSource.new(
            "spec",
            [URI.parse(s.amqp_server.url)],
            "source",
            direct_user: s.users.direct_user
          )

          source.start

          s.vhosts["/"].queue("source").publish(LavinMQ::Message.new("", "", ""))
          expect_raises(AMQP::Client::Connection::ClosedException) do
            source.each do
              s.vhosts["/"].each_connection &.close("spec")
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
            [URI.parse(s.amqp_server.url)],
            "source",
            prefetch: 100,
            direct_user: s.users.direct_user
          )

          source.start

          s.vhosts["/"].queue("source").publish(LavinMQ::Message.new("", "", ""))
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

      it "won't start ack timeout loop when not needed" do
        with_amqp_server do |s|
          source_name = Random::Secure.base64(32)
          source = LavinMQ::Shovel::AMQPSource.new(
            source_name,
            [URI.parse(s.amqp_server.url)],
            "source",
            prefetch: 1,
            direct_user: s.users.direct_user
          )

          source.start

          s.vhosts["/"].queue("source").publish(LavinMQ::Message.new("", "", ""))
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
            [URI.parse(s.amqp_server.url)],
            "source",
            prefetch: 10,
            direct_user: s.users.direct_user,
            batch_ack_timeout: 1.nanosecond
          )

          source.start

          s.vhosts["/"].queue("source").publish(LavinMQ::Message.new("", "", ""))
          wg = WaitGroup.new(1)
          spawn { source.each { |m| source.ack(m.delivery_tag) && wg.done } }
          wg.wait
          sleep 1.millisecond
          s.vhosts["/"].queue("source").unacked_count.should eq 0
          source.stop
        end
      end
    end

    it "will wait to ack all msgs before deleting itself" do
      with_amqp_server do |s|
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "d",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
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
          [URI.parse(s.amqp_server.url)],
          "ql_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
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

    it "respects reject-publish overflow on the destination without losing source messages (#1357)" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "rp_q1",
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
          "rp_q2",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "rp_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("rp_q1")
          args = AMQP::Client::Arguments.new
          args["x-max-length"] = 2_i64
          args["x-overflow"] = "reject-publish"
          q2 = ch.queue("rp_q2", args: args)
          5.times { |i| x.publish_confirm "shovel me #{i}", "rp_q1" }
          spawn shovel.run
          # destination fills to its max-length and stops accepting
          wait_for { q2.message_count == 2 }
          shovel.terminate
          # The bug (#1357) drained the source on overflow, losing messages. The
          # destination must stay capped and the rest must remain on the source —
          # not be acked-and-discarded. (The shovel is at-least-once, so we assert
          # "nothing lost / source not drained", not an exact surviving count.)
          should_eventually(be_true) do
            q2.message_count == 2 && q1.message_count >= 3
          end
        end
      end
    end

    it "does not deadlock when the final message of a queue-length shovel fails delivery" do
      with_amqp_server do |s|
        server = HTTP::Server.new do |context|
          context.response.status_code = 503 # Retry -> reject(requeue: true), never Confirmed
          context.response.print "busy"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "qf_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "qf_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("qf_q1")
          x.publish_confirm "only msg", "qf_q1"
          finished = false
          spawn { shovel.run; finished = true }
          # The final (only) message fails; the shovel must still finish instead
          # of blocking forever on @done.wait waiting for an ack that never comes.
          should_eventually(be_true, 5.seconds) { finished }
        end
      end
    end

    it "should shovel large messages" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "lm_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new("spec", URI.parse(s.amqp_server.url), "lm_q2", direct_user: s.users.direct_user)
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
        source = LavinMQ::Shovel::AMQPSource.new("spec", [URI.parse(s.amqp_server.url)], "sf_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::AMQPDestination.new("spec", URI.parse(s.amqp_server.url), "sf_q2", direct_user: s.users.direct_user)
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "sf_shovel", vhost)
        with_channel(s) do |ch|
          x, q2 = ShovelSpecHelpers.setup_qs ch, "sf_"
          x.publish_confirm "shovel me 1", "sf_q1"
          x.publish_confirm "shovel me 2", "sf_q1"
          spawn shovel.run
          # Buffered so the delivery callback never blocks the connection's
          # read fiber. q2 and x share this connection; if the callback blocked
          # on an unbuffered send, the read fiber couldn't process the publisher
          # confirm for "shovel me 3" below and the example would deadlock.
          msgs = Channel(String).new(8)
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
      ensure
        shovel.try &.terminate
      end
    end

    it "does not let a paused run terminate a resumed shovel" do
      with_amqp_server do |s|
        source = ShovelSpecHelpers::PauseRaceSource.new
        dest = ShovelSpecHelpers::PauseRaceDestination.new
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "pause-race", s.vhosts["/"])
        spawn shovel.run

        wait_for { source.first_each_entered.try_receive? }
        wait_for { shovel.running? }
        shovel.pause
        shovel.resume
        wait_for { source.second_each_entered.try_receive? }
        wait_for { shovel.running? }

        source.release_first_each.send true
        10.times { Fiber.yield }
        shovel.running?.should be_true
      ensure
        shovel.try &.terminate
      end
    end

    it "should shovel with ack mode on-publish" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        ack_mode = LavinMQ::Shovel::AckMode::OnPublish
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "ap_q1",
          prefetch: 1_u16,
          ack_mode: ack_mode,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
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
          s.vhosts["/"].queue("ap_q1").message_count.should eq 0
          q2.get(no_ack: false).try(&.body_io.to_s).should eq "shovel me"
        end
      ensure
        shovel.try &.terminate
      end
    end

    it "should shovel with ack mode no-ack" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        ack_mode = LavinMQ::Shovel::AckMode::NoAck
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "na_q1",
          ack_mode: ack_mode,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
          "na_q2",
          ack_mode: ack_mode,
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "na_shovel", vhost)
        with_channel(s) do |ch|
          x, q2 = ShovelSpecHelpers.setup_qs ch, "na_"
          x.publish_confirm "shovel me", "na_q1"
          spawn { shovel.run }
          wait_for { s.vhosts["/"].queue("na_q1").message_count.zero? }
          wait_for { !s.vhosts["/"].queue("na_q2").message_count.zero? }
          q2.get(no_ack: false).try(&.body_io.to_s).should eq "shovel me"
        end
      ensure
        shovel.try &.terminate
      end
    end

    it "should shovel past prefetch" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "prefetch_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          prefetch: 21_u16,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
          "prefetch_q2",
          direct_user: s.users.direct_user
        )
        with_channel(s) do |ch|
          x = ShovelSpecHelpers.setup_qs(ch, "prefetch_").first
          ch.confirm_select
          100.times do
            x.publish "shovel me", "prefetch_q1"
          end
          ch.wait_for_confirms
          wait_for { s.vhosts["/"].queue("prefetch_q1").message_count == 100 }
          shovel = LavinMQ::Shovel::Runner.new(source, dest, "prefetch_shovel", vhost)
          shovel.run
          wait_for { shovel.terminated? }
          s.vhosts["/"].queue("prefetch_q1").message_count.should eq 0
          s.vhosts["/"].queue("prefetch_q2").message_count.should eq 100
        end
      end
    end

    it "should shovel once qs are declared" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "od_q1",
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
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
      ensure
        shovel.try &.terminate
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
        "src-uri": "#{s.amqp_server.url}",
        "src-queue": "rc_q1",
        "dest-uri": "#{s.amqp_server.url}",
        "dest-queue": "rc_q2",
        "src-prefetch-count": 2})
        p = LavinMQ::Parameter.new("shovel", "rc_shovel", JSON.parse(config))
        s.vhosts["/"].add_parameter(p)
        restart_server(s)
        wait_for { s.vhosts["/"].shovels.size > 0 }
        shovel = s.vhosts["/"].shovels["rc_shovel"]
        wait_for { shovel.running? }
        with_channel(s) do |ch|
          q1 = ch.queue("rc_q1", durable: true)
          q2 = ch.queue("rc_q2", durable: true)
          # Buffered: q1 and q2 share this connection, so a delivery callback
          # blocking on an unbuffered send would stall the read fiber and
          # deadlock the publish_confirms below.
          msgs = Channel(String).new(8)
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
          [URI.parse("#{s.amqp_server.url.sub("amqp://", "amqps://")}?verify=none")],
          "ssl_q1",
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse("#{s.amqp_server.url.sub("amqp://", "amqps://")}?verify=none"),
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
      ensure
        shovel.try &.terminate
      end
    end

    it "should ack all messages that has been moved" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        # Use prefetch 5, then the batch ack will ack every third message
        prefetch = 5_u16
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "prefetch2_q1",
          prefetch: prefetch,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
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
          wait_for { s.vhosts["/"].queue("prefetch2_q2").message_count == 4 }
          # ... but only three has been acked (because batching)
          wait_for { s.vhosts["/"].queue("prefetch2_q1").unacked_count == 1 }
          # The source only registers the last delivery as unacked once the
          # destination's publish confirm round-trips back. Until then a
          # terminate would (correctly) requeue the unconfirmed message rather
          # than ack it, so wait for that confirm before terminating — otherwise
          # this races under load and leaves a message on q1.
          wait_for { source.last_unacked == 4_u64 }
          # Now when we terminate the shovel it should ack the last message(s)
          shovel.terminate
          wait_for { s.vhosts["/"].queue("prefetch2_q1").unacked_count == 0 }
          s.vhosts["/"].queue("prefetch2_q2").message_count.should eq 4
          s.vhosts["/"].queue("prefetch2_q1").message_count.should eq 0
        end
      end
    end

    describe "authentication error" do
      it "should be stopped" do
        with_amqp_server do |s|
          vhost = s.vhosts.create("x")
          uri = URI.parse(s.amqp_server.url)
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
          shovel.state.to_s.should eq "Terminated"
        end
      end
    end

    it "should count messages shoveled" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("x")
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "c_q1",
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
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
          wait_for { s.vhosts["/"].queue("c_q2").message_count == 10 }
          shovel.details_tuple[:message_count].should eq 10
        end
        shovel.state.to_s.should eq "Running"
      ensure
        shovel.try &.terminate
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
          [URI.parse(s.amqp_server.url)],
          q1_name,
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user,
          consumer_args: consumer_args
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec",
          URI.parse(s.amqp_server.url),
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
          [URI.parse(s.amqp_server.url)],
          "#{long_prefix}q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "#{long_prefix}q2",
          URI.parse(s.amqp_server.url),
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

    it "should pause and resume shovel", tags: "slow" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("pause:resume:vhost")
        queue_name = "shovel:pause:resume"
        source = LavinMQ::Shovel::AMQPSource.new(
          "#{queue_name}q1",
          [URI.parse(s.amqp_server.url)],
          "#{queue_name}q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::Never,
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "#{queue_name}q2",
          URI.parse(s.amqp_server.url),
          "#{queue_name}q2",
          direct_user: s.users.direct_user
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "pause:resume:shovel", vhost)
        with_channel(s) do |ch|
          q1, q2 = ShovelSpecHelpers.setup_qs ch, queue_name
          q1.publish_confirm "shovel me 1", "#{queue_name}q1"
          q1.publish_confirm "shovel me 2", "#{queue_name}q1"
          spawn { shovel.run }
          wait_for { s.vhosts["/"].queue("#{queue_name}q2").message_count == 2 }
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 1"
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 2"
          # Wait until the source has durably acked the two moved messages
          # before pausing. With the default prefetch they're only batched as
          # unacked until the dest confirms land; if pause closes the connection
          # first they're (correctly) requeued and re-shoveled after resume,
          # duplicating them on q2. This races under load on macOS CI.
          wait_for { s.vhosts["/"].queue("#{queue_name}q1").unacked_count.zero? }
          shovel.pause
          shovel.paused?.should eq true

          q1.publish_confirm "shovel me 3", "#{queue_name}q1"
          q1.publish_confirm "shovel me 4", "#{queue_name}q1"
          q2.get(no_ack: true).try(&.body_io.to_s).should eq nil

          spawn shovel.resume
          wait_for { shovel.running? } # Ensure it gets back to Running state
          wait_for { s.vhosts["/"].queue("#{queue_name}q2").message_count == 2 }
          shovel.terminate
          wait_for { shovel.terminated? }
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 3"
          q2.get(no_ack: true).try(&.body_io.to_s).should eq "shovel me 4"
          s.vhosts["/"].shovels.empty?.should be_true
        end
      end
    end

    it "should pause and resume shovel on long queue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        queue_name = "shovel:pause:resume"
        message_count = 10_000
        with_channel(s) do |ch|
          q1, _q2 = ShovelSpecHelpers.setup_qs ch, queue_name
          message_count.times do |i|
            q1.publish "msg #{i}", "#{queue_name}q1"
          end

          config = %({
            "src-uri": "#{s.amqp_server.url}",
            "src-queue": "#{queue_name}q1",
            "dest-uri": "#{s.amqp_server.url}",
            "dest-queue": "#{queue_name}q2",
            "src-delete-after": "queue-length",
            "src-prefetch-count": 2})
          p = LavinMQ::Parameter.new("shovel", queue_name, JSON.parse(config))
          vhost.add_parameter(p)
          wait_for { vhost.shovels.size > 0 }
          shovel = vhost.shovels[queue_name]
          wait_for { shovel.running? }
          shovel.pause
          wait_for { shovel.paused? }

          vhost.queue("#{queue_name}q1").message_count.should be > 0
          shovel = vhost.shovels[queue_name]
          spawn shovel.resume
          wait_for { shovel.running? } # Ensure it gets back to Running state
          vhost.delete_parameter("shovel", queue_name)
          vhost.shovels.size.should eq 0
        end
      end
    end

    it "should keep paused even on broker restarts" do
      with_amqp_server do |s|
        vhost = s.vhosts.create("pause:resume:vhost")
        shovel_name = "shovel:pause:resume"
        config = %({
        "src-uri": "#{s.amqp_server.url}",
        "src-queue": "#{shovel_name}_q1",
        "dest-uri": "#{s.amqp_server.url}",
        "dest-queue": "#{shovel_name}_q2" })
        p = LavinMQ::Parameter.new("shovel", shovel_name, JSON.parse(config))
        s.vhosts[vhost.name].add_parameter(p)
        shovel = s.vhosts[vhost.name].shovels[shovel_name]
        shovel.pause
        shovel.paused?.should eq true
        restart_server(s)
        should_eventually(be_true) { s.vhosts[vhost.name].shovels[shovel.name].paused? }
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
          [URI.parse(s.amqp_server.url)],
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

    it "requeues the message to the source when the HTTP destination returns an error (#1612)" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          received.add(1)
          context.response.status_code = 404
          context.response.print "not found"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec",
          [URI.parse(s.amqp_server.url)],
          "err_q1",
          direct_user: s.users.direct_user
        )
        dest = LavinMQ::Shovel::HTTPDestination.new(
          "spec",
          URI.parse("http://#{addr}/")
        )
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "err_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("err_q1")
          x.publish_confirm "shovel me", "err_q1"
          spawn shovel.run
          wait_for { received.get >= 1 }
          shovel.terminate
          # a failed HTTP delivery must not drop the message; it stays in the source
          should_eventually(eq 1) { q1.message_count }
        end
      end
    end

    it "dead-letters via the source DLX when the HTTP destination returns 400 (#5 Reject)" do
      with_amqp_server do |s|
        server = HTTP::Server.new do |context|
          context.response.status_code = 400
          context.response.print "bad request"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "rej_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "rej_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          dlq = ch.queue("rej_dlq")
          dlq.bind("amq.fanout", "")
          args = AMQP::Client::Arguments.new
          args["x-dead-letter-exchange"] = "amq.fanout"
          q1 = ch.queue("rej_q1", args: args)
          x.publish_confirm "bad msg", "rej_q1"
          spawn shovel.run
          # 400 = bad message: rejected without requeue, so the source DLX takes it
          should_eventually(eq 1) { dlq.message_count }
          q1.message_count.should eq 0
          shovel.terminate
        end
      end
    end

    it "fast-retries a transient 503 in place and delivers without requeueing" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          n = received.add(1) # returns the count before this request
          context.response.status_code = n < 2 ? 503 : 200
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "fr_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "fr_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("fr_q1")
          x.publish_confirm "retry me", "fr_q1"
          # Two 503s then a 200, all within one push: the blip is absorbed in
          # place, so the delivery is Confirmed and the message is never requeued.
          shovel.run
          received.get.should eq 3
          d = shovel.details_tuple
          d[:confirmed].should eq 1
          d[:retried].should eq 0
        end
      end
    end

    it "retries transport-level timeouts in place (honouring dest-timeout) and recovers" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          n = received.add(1)
          sleep 600.milliseconds if n < 2 # first two attempts exceed the 200ms timeout
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "to_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new(
          "spec", URI.parse("http://#{addr}/"), timeout: 200.milliseconds)
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "to_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("to_q1")
          x.publish_confirm "slow me", "to_q1"
          # The first two attempts read-timeout at 200ms (server holds 600ms); each
          # timeout closes and reopens the client and counts as a transient failure
          # retried in place. The third attempt is fast and Confirms — never requeued.
          shovel.run
          received.get.should be >= 3
          d = shovel.details_tuple
          d[:confirmed].should eq 1
          d[:retried].should eq 0
        end
      end
    end

    it "bounds in-place retries to 1 + MAX_RETRIES then requeues when the 503 persists (#5 Retry)" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          received.add(1)
          context.response.status_code = 503
          context.response.print "unavailable"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "rt_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "rt_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("rt_q1")
          x.publish_confirm "retry me", "rt_q1"
          # A persistent 503: one push makes exactly 1 + MAX_RETRIES in-place
          # attempts, then reports Retry so the Runner requeues the message. The
          # endpoint sees a bounded burst, not a busy-loop of hundreds/sec.
          shovel.run
          received.get.should eq 6
          shovel.details_tuple[:retried].should eq 1
          # the message is never lost: it's back on the source queue
          should_eventually(eq 1) { q1.message_count }
        end
      end
    end

    it "classifies a TLS handshake failure as a transient (Retry) outcome, not an unhandled error (#3)" do
      with_amqp_server do |s|
        # A plaintext HTTP server; connecting to it over TLS fails the handshake,
        # which surfaces as OpenSSL::SSL::Error rather than an IO/Socket error.
        server = HTTP::Server.new do |context|
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "tls_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new(
          "spec", URI.parse("https://#{addr}/"), timeout: 200.milliseconds)
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "tls_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("tls_q1")
          x.publish_confirm "deliver me", "tls_q1"
          spawn shovel.run
          # The TLS error must be caught and classified as Retry (requeue). Before
          # the fix it escaped the rescue as an unhandled exception, driving the
          # runner's reconnect path instead — so `retried` would stay 0.
          should_eventually(be_true, 5.seconds) { shovel.details_tuple[:retried] >= 1 }
          shovel.state.error?.should be_false
          shovel.terminate
          should_eventually(eq 1) { q1.message_count }
        end
      end
    end

    it "reports Retry (and does not hang) when an on-publish HTTP destination is unreachable" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "op_q1", direct_user: s.users.direct_user)
        # Port 1 refuses connections, so every POST fails at the transport level.
        dest = LavinMQ::Shovel::HTTPDestination.new(
          "spec", URI.parse("http://127.0.0.1:1/"),
          LavinMQ::Shovel::AckMode::OnPublish, timeout: 200.milliseconds)
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "op_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("op_q1")
          x.publish_confirm "deliver me", "op_q1"
          spawn shovel.run
          # A failed on-publish POST must be reported as Retry (requeue), not spin
          # in an unbounded loop and not be silently Confirmed.
          should_eventually(be_true, 3.seconds) { shovel.details_tuple[:retried] >= 1 }
          shovel.details_tuple[:confirmed].should eq 0
          shovel.terminate
          should_eventually(eq 1) { q1.message_count }
        end
      end
    end

    it "errors-out the shovel after repeated Abort responses from the HTTP destination (#5 Abort)" do
      with_amqp_server do |s|
        server = HTTP::Server.new do |context|
          context.response.status_code = 404
          context.response.print "not found"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "ab_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ab_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("ab_q1")
          x.publish_confirm "no route", "ab_q1"
          spawn shovel.run
          # 404 = endpoint unusable: after a threshold of consecutive Aborts the
          # shovel errors out for an operator to resolve, rather than looping.
          should_eventually(be_true) { shovel.state.error? }
          should_eventually(eq 1) { q1.message_count }
        end
      end
    end

    it "stops delivering once the shovel is paused, mid-stream (#1612 part 2 / #5.4)" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          received.add(1)
          sleep 0.3.seconds
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "pf_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "pf_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("pf_q1")
          6.times { |i| x.publish_confirm "m#{i}", "pf_q1" }
          spawn shovel.run
          wait_for { received.get >= 1 } # first delivery in-flight
          shovel.pause
          shovel.state.paused?.should be_true
          # Delivery must halt promptly: at most an in-flight/buffered straggler
          # drains, then it stops. The bug let retries continue after pause.
          sleep 1.second
          settled = received.get
          sleep 1.second
          received.get.should eq settled # no ongoing retries after pause
          settled.should be < 6          # halted mid-stream, didn't drain
        end
      end
    end

    it "fails over to the next destination when the active one is unusable (#4)" do
      with_amqp_server do |s|
        bad_received = Atomic(Int32).new(0)
        bad = HTTP::Server.new do |context|
          bad_received.add(1)
          context.response.status_code = 404
          context.response.print "no route"
          context
        end
        bad_addr = bad.bind_unused_port
        spawn bad.listen

        good_received = Atomic(Int32).new(0)
        good = HTTP::Server.new do |context|
          good_received.add(1)
          context.response.print "ok"
          context
        end
        good_addr = good.bind_unused_port
        spawn good.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "fo_q1", direct_user: s.users.direct_user)
        dest_a = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{bad_addr}/"))
        dest_b = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{good_addr}/"))
        multi = LavinMQ::Shovel::MultiDestinationHandler.new([dest_a, dest_b] of LavinMQ::Shovel::Destination)
        shovel = LavinMQ::Shovel::Runner.new(source, multi, "fo_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("fo_q1")
          x.publish_confirm "deliver me", "fo_q1"
          spawn shovel.run
          # A (404) is tried first and is unusable, so the shovel fails over to B
          should_eventually(eq 1) { good_received.get }
          bad_received.get.should be >= 1
          shovel.terminate
        end
      end
    end

    it "does not error-out when aborts are interleaved with other outcomes (#review)" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          old = received.add(1)
          # alternate 404 (Abort) and 400 (Reject) — never persistently unusable
          context.response.status_code = old.even? ? 404 : 400
          context.response.print "x"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "ir_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ir_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("ir_q1")
          30.times { |i| x.publish_confirm "m#{i}", "ir_q1" }
          spawn shovel.run
          # Each abort is interrupted by a non-abort outcome, so the consecutive
          # abort counter never reaches the threshold; the shovel keeps running
          # instead of erroring out as if the destination were unusable.
          should_eventually(be_true) { received.get >= 30 }
          shovel.state.error?.should be_false
          shovel.terminate
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
          [URI.parse(s.amqp_server.url)],
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

  describe "HTTPDestination dest-timeout" do
    it "defaults to 30 seconds" do
      LavinMQ::Shovel::HTTPDestination.timeout_from(JSON.parse("{}")).should eq 30.seconds
      dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://localhost/"))
      dest.timeout.should eq 30.seconds
    end

    it "parses dest-timeout given as seconds (int or float)" do
      LavinMQ::Shovel::HTTPDestination.timeout_from(JSON.parse(%({"dest-timeout": 5}))).should eq 5.seconds
      LavinMQ::Shovel::HTTPDestination.timeout_from(JSON.parse(%({"dest-timeout": 2.5}))).should eq 2.5.seconds
    end

    it "falls back to the default for non-positive values" do
      LavinMQ::Shovel::HTTPDestination.timeout_from(JSON.parse(%({"dest-timeout": 0}))).should eq 30.seconds
      LavinMQ::Shovel::HTTPDestination.timeout_from(JSON.parse(%({"dest-timeout": -3}))).should eq 30.seconds
    end

    it "wires the configured dest-timeout through the store to HTTP deliveries" do
      with_amqp_server do |s|
        served = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          served.add(1)
          sleep 1.second # always slower than the configured 0.2s dest-timeout
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("ct_q1")
          x.publish_confirm "hi", "ct_q1"
          config = %({
            "src-uri": "#{s.amqp_server.url}",
            "src-queue": "ct_q1",
            "dest-uri": "http://#{addr}/",
            "dest-timeout": 0.2})
          vhost.add_parameter(LavinMQ::Parameter.new("shovel", "ct_shovel", JSON.parse(config)))
          # With the 0.2s timeout wired through, each attempt times out long before
          # the server's 1s response and is retried, so the endpoint is hit
          # repeatedly. With the old hard-coded 30s timeout the first attempt would
          # simply wait 1s, succeed, and never retry (served would stay 1).
          should_eventually(be_true, 3.seconds) { served.get >= 2 }
          vhost.delete_parameter("shovel", "ct_shovel")
        end
      end
    end
  end

  describe "runtime counters" do
    it "counts confirmed deliveries and exposes degraded fields" do
      with_amqp_server do |s|
        server = HTTP::Server.new do |context|
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "rc_ok_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "rc_ok_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("rc_ok_q1")
          3.times { |i| x.publish_confirm "m#{i}", "rc_ok_q1" }
          shovel.run
          d = shovel.details_tuple
          d[:confirmed].should eq 3
          d[:dead_lettered].should eq 0
          d[:aborted].should eq 0
          d[:consecutive_failures].should eq 0
          d[:consecutive_aborts].should eq 0
          d[:abort_threshold].should eq 10
        end
      end
    end

    it "counts dead-lettered (Reject) deliveries" do
      with_amqp_server do |s|
        server = HTTP::Server.new do |context|
          context.response.status_code = 400
          context.response.print "bad"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "rc_rej_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "rc_rej_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("rc_rej_q1")
          3.times { |i| x.publish_confirm "m#{i}", "rc_rej_q1" }
          shovel.run
          d = shovel.details_tuple
          d[:dead_lettered].should eq 3
          d[:confirmed].should eq 0
        end
      end
    end
  end

  describe "delivery backoff" do
    it "ramps 0.5s, doubling, capped at 30s" do
      LavinMQ::Shovel::Runner.delivery_backoff(0).should eq 0.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(1).should eq 0.5.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(2).should eq 1.second
      LavinMQ::Shovel::Runner.delivery_backoff(3).should eq 2.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(4).should eq 4.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(5).should eq 8.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(6).should eq 16.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(7).should eq 30.seconds
      LavinMQ::Shovel::Runner.delivery_backoff(50).should eq 30.seconds
    end
  end

  describe "Store.validate_config!" do
    it "looks up vhost permissions by bare name (strips leading slash from URI path)" do
      with_amqp_server do |s|
        user = s.users.create("shovel_user", "pass")
        s.users.add_permission("shovel_user", "test", /.*/, /.*/, /.*/)
        config = JSON.parse({
          "src-uri":    "amqp:///test",
          "dest-uri":   "amqp:///test",
          "src-queue":  "q1",
          "dest-queue": "q2",
        }.to_json)
        LavinMQ::Shovel::Store.validate_config!(config, user)
      end
    end

    it "raises when user lacks permission on the named vhost" do
      with_amqp_server do |s|
        user = s.users.create("shovel_user2", "pass")
        s.users.add_permission("shovel_user2", "/", /.*/, /.*/, /.*/)
        config = JSON.parse({
          "src-uri":    "amqp:///test",
          "dest-uri":   "amqp:///test",
          "src-queue":  "q1",
          "dest-queue": "q2",
        }.to_json)
        expect_raises(LavinMQ::Shovel::ConfigError) do
          LavinMQ::Shovel::Store.validate_config!(config, user)
        end
      end
    end

    it "allows an HTTP destination without a dest queue or exchange" do
      config = JSON.parse({
        "src-uri":   "amqp:///test",
        "src-queue": "q1",
        "dest-uri":  "http://example.com/hook",
      }.to_json)
      LavinMQ::Shovel::Store.validate_config!(config, nil)
    end

    it "still requires a dest queue or exchange for an AMQP destination" do
      config = JSON.parse({
        "src-uri":   "amqp:///test",
        "src-queue": "q1",
        "dest-uri":  "amqp:///test",
      }.to_json)
      expect_raises(LavinMQ::Shovel::ConfigError, "destination requires") do
        LavinMQ::Shovel::Store.validate_config!(config, nil)
      end
    end
  end

  describe "MultiDestinationHandler" do
    it "fails over to the next destination and retries on a single Abort" do
      a = ShovelSpecHelpers::StubDestination.new
      b = ShovelSpecHelpers::StubDestination.new
      parent = ShovelSpecHelpers::RecordingListener.new
      multi = LavinMQ::Shovel::MultiDestinationHandler.new(
        [a, b] of LavinMQ::Shovel::Destination)
      multi.listener = parent
      multi.start
      multi.report(7_u64, LavinMQ::Shovel::Outcome::Abort)
      parent.outcomes.should eq [{7_u64, LavinMQ::Shovel::Outcome::Retry}]
    end

    it "propagates Abort once every destination has aborted in a row" do
      a = ShovelSpecHelpers::StubDestination.new
      b = ShovelSpecHelpers::StubDestination.new
      parent = ShovelSpecHelpers::RecordingListener.new
      multi = LavinMQ::Shovel::MultiDestinationHandler.new(
        [a, b] of LavinMQ::Shovel::Destination)
      multi.listener = parent
      multi.start
      multi.report(7_u64, LavinMQ::Shovel::Outcome::Abort)
      multi.report(7_u64, LavinMQ::Shovel::Outcome::Abort)
      parent.outcomes.should eq [
        {7_u64, LavinMQ::Shovel::Outcome::Retry},
        {7_u64, LavinMQ::Shovel::Outcome::Abort},
      ]
    end

    it "forwards a non-Abort outcome and resets the abort streak" do
      a = ShovelSpecHelpers::StubDestination.new
      b = ShovelSpecHelpers::StubDestination.new
      parent = ShovelSpecHelpers::RecordingListener.new
      multi = LavinMQ::Shovel::MultiDestinationHandler.new(
        [a, b] of LavinMQ::Shovel::Destination)
      multi.listener = parent
      multi.start
      multi.report(1_u64, LavinMQ::Shovel::Outcome::Abort)     # streak 1, fail over to b
      multi.report(2_u64, LavinMQ::Shovel::Outcome::Confirmed) # forwarded, streak reset
      multi.report(3_u64, LavinMQ::Shovel::Outcome::Abort)     # streak 1 again, not >= size
      parent.outcomes.should eq [
        {1_u64, LavinMQ::Shovel::Outcome::Retry},
        {2_u64, LavinMQ::Shovel::Outcome::Confirmed},
        {3_u64, LavinMQ::Shovel::Outcome::Retry},
      ]
    end
  end
end
