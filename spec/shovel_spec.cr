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

    def started? : Bool
      true
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

  # A destination whose start can be made to fail, recording start/stop calls,
  # so MultiDestinationHandler's failover order can be asserted.
  class FlakyStartDestination < LavinMQ::Shovel::Destination
    property start_error : Exception?
    getter starts = 0
    getter stops = 0
    getter pushes = 0
    @started = false

    def initialize(@start_error : Exception? = nil)
    end

    def start
      @starts += 1
      if err = @start_error
        raise err
      end
      @started = true
    end

    def stop
      @stops += 1
      @started = false
    end

    def push(msg)
      @pushes += 1
    end

    def started? : Bool
      @started
    end
  end

  # Like FlakyStartDestination, but stopping it voids its in-flight confirms
  # the way amqp-client does on a connection close: each pending tag is
  # reported as Retry from within stop.
  class VoidingDestination < FlakyStartDestination
    property pending = [] of UInt64

    def stop
      super
      pending, @pending = @pending, [] of UInt64
      pending.each { |tag| @listener.report(tag, LavinMQ::Shovel::Outcome::Retry) }
    end
  end

  # A source that is never started and records every settlement, for testing
  # the Runner's outcome handling in isolation.
  class StoppedSource < LavinMQ::Shovel::Source
    getter delete_after = LavinMQ::Shovel::DeleteAfter::Never
    getter settlements = [] of {UInt64, Symbol}

    def start
    end

    def stop
    end

    def started? : Bool
      false
    end

    def each(&_blk : ::AMQP::Client::DeliverMessage -> Nil)
    end

    def ack(delivery_tag, batch = true)
      @settlements << {delivery_tag, :ack}
    end

    def reject(delivery_tag, requeue)
      @settlements << {delivery_tag, requeue ? :requeue : :reject}
    end
  end

  # A delivery as the Runner would hand it to a Destination.
  def self.message(ch, delivery_tag : UInt64, body = "m") : AMQP::Client::DeliverMessage
    AMQP::Client::DeliverMessage.new(ch, "", "q", delivery_tag, AMQ::Protocol::Properties.new, IO::Memory.new(body), false)
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
          spawn { source.each { |m| source.ack(m.delivery_tag); wg.done } }
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

    it "reports Retry for confirms voided by a connection close so they are requeued" do
      with_amqp_server do |s|
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec", URI.parse(s.amqp_server.url), "pc_q2", direct_user: s.users.direct_user)
        listener = ShovelSpecHelpers::RecordingListener.new
        dest.listener = listener
        with_channel(s) do |ch|
          ch.queue("pc_q2")
          dest.start
          50.times do |i|
            dest.push(ShovelSpecHelpers.message(ch, i.to_u64 + 1, "m#{i}"))
          end
          # amqp-client voids every pending confirm with `false` when the
          # connection goes. On a failover the source stays open, so every
          # in-flight message must be requeued there — otherwise a later
          # cumulative ack sweeps it away undelivered. When the whole shovel is
          # stopping the source is already closed and the Runner ignores it.
          dest.@ch.not_nil!.cleanup
          voided = listener.outcomes.select { |(_, outcome)| outcome.retry? }
          voided.size.should eq(50 - listener.outcomes.count { |(_, outcome)| outcome.confirmed? })
        end
        dest.stop
      end
    end

    it "requeues in-flight messages on the source when failing over between AMQP destinations" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "fa_q1", direct_user: s.users.direct_user)
        dest_a = LavinMQ::Shovel::AMQPDestination.new(
          "spec", URI.parse(s.amqp_server.url), "fa_qa", direct_user: s.users.direct_user)
        dest_b = LavinMQ::Shovel::AMQPDestination.new(
          "spec", URI.parse(s.amqp_server.url), "fa_qb", direct_user: s.users.direct_user)
        multi = LavinMQ::Shovel::MultiDestinationHandler.new([dest_a, dest_b] of LavinMQ::Shovel::Destination)
        shovel = LavinMQ::Shovel::Runner.new(source, multi, "fa_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("fa_q1")
          args = AMQP::Client::Arguments.new
          args["x-max-length"] = 1_i64
          args["x-overflow"] = "reject-publish"
          qa = ch.queue("fa_qa", args: args)
          qb = ch.queue("fa_qb")
          6.times { |i| x.publish_confirm "shovel me #{i}", "fa_q1" }
          spawn shovel.run
          # A takes one message and nacks the rest (reject-publish overflow).
          # Three nacks in a row fail the shovel over to B; the failover happens
          # on the Runner fiber, not inside the publisher-confirm callback where
          # closing A's connection would wait on the very fiber running it.
          should_eventually(eq(5), 5.seconds) { qb.message_count }
          qa.message_count.should eq 1
          should_eventually(eq 0) { q1.message_count }
          shovel.terminate
        end
      end
    end

    it "keeps retrying the final message of a queue-length shovel instead of finishing without it" do
      with_amqp_server do |s|
        server = HTTP::Server.new do |context|
          context.request.body.try &.skip_to_end
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
          q1 = ch.queue("qf_q1")
          x.publish_confirm "only msg", "qf_q1"
          finished = false
          spawn { shovel.run; finished = true }
          # A requeued message is redelivered and retried with backoff. The run
          # must neither hang forever nor declare the queue drained (and delete
          # the shovel) while the message is still on the source.
          should_eventually(be_true, 5.seconds) { shovel.details_tuple[:retried] >= 2 }
          finished.should be_false
          shovel.terminate
          should_eventually(be_true, 5.seconds) { finished }
          should_eventually(eq 1) { q1.message_count }
        end
      end
    end

    it "moves a message published after the start rather than leaving it unacked to be swept away" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "nw_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("nw_q1")
          delivered = [] of String
          requests = Atomic(Int32).new(0)
          server = HTTP::Server.new do |context|
            body = context.request.body.try(&.gets_to_end).to_s
            case requests.add(1)
            when 0 # m1: delivered, and a newer message lands on the queue meanwhile
              x.publish_confirm "newer", "nw_q1"
              context.response.status_code = 200
              delivered << body
            when 1 # m2 fails once and is requeued
              context.response.status_code = 503
            else
              context.response.status_code = 200
              delivered << body
            end
            context.response.print "x"
            context
          end
          addr = server.bind_unused_port
          spawn server.listen
          dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
          shovel = LavinMQ::Shovel::Runner.new(source, dest, "nw_shovel", vhost)
          x.publish_confirm "m1", "nw_q1"
          x.publish_confirm "m2", "nw_q1"
          shovel.run
          # "newer" was delivered into the slot m1 freed. Skipping it would leave
          # it unacked, and the cumulative ack for m2's redelivery (a higher
          # tag) would then settle it without it ever having been delivered.
          # Queue-length moves as many messages as were on the queue at start;
          # whatever is not moved must still be on the source.
          should_eventually(eq 0) { s.vhosts["/"].queue("nw_q1").unacked_count }
          left = q1.message_count
          (delivered.size + left).should eq 3
          delivered.uniq.size.should eq delivered.size
          left_bodies = Array(String).new(left) { q1.get(no_ack: true).not_nil!.body_io.to_s }
          (delivered + left_bodies).sort.should eq ["m1", "m2", "newer"]
        ensure
          server.try &.close
        end
      end
    end

    it "moves every message of a queue-length shovel across a pause and resume" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        third_started = Channel(Nil).new
        release_third = Channel(Nil).new
        server = HTTP::Server.new do |context|
          context.request.body.try &.skip_to_end
          if received.add(1) == 2 # hold the third request open until the shovel is paused
            third_started.send(nil)
            release_third.receive
          end
          context.response.status_code = 200
          context.response.print "x"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "pr_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          prefetch: 1_u16,
          direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "pr_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("pr_q1")
          4.times { |i| x.publish_confirm "m#{i}", "pr_q1" }
          spawn shovel.run
          third_started.receive
          shovel.pause # m1, m2 settled; m3 in flight is requeued; m3, m4 remain
          release_third.send(nil)
          # The resumed run takes a fresh snapshot of what is left. Counting the
          # messages settled before the pause against it would finish the run —
          # and delete the shovel — with messages still on the queue.
          shovel.resume
          should_eventually(be_true, 5.seconds) { shovel.terminated? }
          q1.message_count.should eq 0
          received.get.should be >= 4
        end
      ensure
        server.try &.close
      end
    end

    it "finishes a queue-length shovel when the broker drops a requeued message" do
      with_amqp_server do |s|
        server = HTTP::Server.new do |context|
          context.request.body.try &.skip_to_end
          context.response.status_code = 503 # Retry: reject(requeue: true)
          context.response.print "busy"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "dl_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user, batch_ack_timeout: 100.milliseconds)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "dl_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          args = AMQP::Client::Arguments.new
          args["x-delivery-limit"] = 0_i64 # the first requeue dead-letters (here: drops) the message
          q1 = ch.queue("dl_q1", args: args)
          x.publish_confirm "doomed", "dl_q1"
          finished = false
          spawn { shovel.run; finished = true }
          # The requeued message never comes back, so counting settlements alone
          # would leave the shovel Running forever on an empty queue.
          should_eventually(be_true, 5.seconds) { finished }
          q1.message_count.should eq 0
        end
      ensure
        server.try &.close
      end
    end

    it "only acks up to the lowest unconfirmed tag when confirms arrive out of order" do
      with_amqp_server do |s|
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "oo_q1",
          prefetch: 3_u16, direct_user: s.users.direct_user, batch_ack_timeout: 50.milliseconds)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("oo_q1")
          3.times { |i| x.publish_confirm "m#{i}", "oo_q1" }
          q1 = s.vhosts["/"].queue("oo_q1")
          source.start
          spawn { source.each { } rescue nil }
          should_eventually(eq 3) { q1.unacked_count }
          # A RabbitMQ destination may confirm 1 and 3 before 2. Acks are
          # cumulative, so the source may only ack up to 1 until 2 is confirmed;
          # acking 3 would settle 2 before anyone has delivered it.
          source.ack(1_u64)
          source.ack(3_u64)
          sleep 200.milliseconds # a couple of ack-timeout flushes
          q1.unacked_count.should eq 2
          source.ack(2_u64)
          should_eventually(eq 0) { q1.unacked_count }
        end
        source.stop
      end
    end

    it "finishes a queue-length shovel only once every message is delivered, retries included" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          context.request.body.try &.skip_to_end
          # the second request fails once; everything else succeeds
          context.response.status_code = received.add(1) == 1 ? 503 : 200
          context.response.print "x"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "qr_q1",
          delete_after: LavinMQ::Shovel::DeleteAfter::QueueLength,
          direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "qr_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("qr_q1")
          3.times { |i| x.publish_confirm "m#{i}", "qr_q1" }
          shovel.run
          # The redelivery carries a delivery tag past the snapshot, but it is
          # one of the snapshot's messages: it must be delivered before the run
          # counts as done, not skipped while the shovel deletes itself.
          d = shovel.details_tuple
          d[:confirmed].should eq 3
          d[:retried].should eq 1
          received.get.should eq 4
          q1.message_count.should eq 0
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
        config = <<-JSON
          {
            "src-uri": "#{s.amqp_server.url}",
            "src-queue": "rc_q1",
            "dest-uri": "#{s.amqp_server.url}",
            "dest-queue": "rc_q2",
            "src-prefetch-count": 2
          }
          JSON
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
          wait_for { source.pending_ack == 4_u64 }
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
          shovel.paused?.should be_true

          q1.publish_confirm "shovel me 3", "#{queue_name}q1"
          q1.publish_confirm "shovel me 4", "#{queue_name}q1"
          q2.get(no_ack: true).try(&.body_io.to_s).should be_nil

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

          config = <<-JSON
            {
              "src-uri": "#{s.amqp_server.url}",
              "src-queue": "#{queue_name}q1",
              "dest-uri": "#{s.amqp_server.url}",
              "dest-queue": "#{queue_name}q2",
              "src-delete-after": "queue-length",
              "src-prefetch-count": 2
            }
            JSON
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
        config = <<-JSON
          {
            "src-uri": "#{s.amqp_server.url}",
            "src-queue": "#{shovel_name}_q1",
            "dest-uri": "#{s.amqp_server.url}",
            "dest-queue": "#{shovel_name}_q2"
          }
          JSON
        p = LavinMQ::Parameter.new("shovel", shovel_name, JSON.parse(config))
        s.vhosts[vhost.name].add_parameter(p)
        shovel = s.vhosts[vhost.name].shovels[shovel_name]
        shovel.pause
        shovel.paused?.should be_true
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
          # The body size is known up front, so the request carries a
          # Content-Length rather than chunked transfer encoding.
          h["Content-Length"].should eq "shovel me".bytesize.to_s
          h.has_key?("Transfer-Encoding").should be_false

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

    it "requeues the message when the HTTP destination returns 503 (#5 Retry)" do
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
          "spec", [URI.parse(s.amqp_server.url)], "rt_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "rt_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("rt_q1")
          x.publish_confirm "retry me", "rt_q1"
          # A 503 is the endpoint answering, not a dead connection, so there is
          # no in-place retry: one attempt, then Retry so the Runner requeues the
          # message and backs off. The endpoint sees paced attempts, not a
          # busy-loop of hundreds per second.
          spawn shovel.run
          should_eventually(be_true) { shovel.details_tuple[:retried] >= 1 }
          received.get.should be <= 2
          shovel.terminate
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

    it "keeps delivering after a transport failure instead of raising Not started" do
      with_amqp_server do |s|
        # The handler never reads the request body, so the server closes the
        # connection after every response: the next request on the kept-alive
        # socket fails at the transport level (a stale keep-alive).
        server = HTTP::Server.new do |context|
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "ns_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "ns_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("ns_q1")
          3.times { |i| x.publish_confirm "m#{i}", "ns_q1" }
          spawn shovel.run
          # A stale socket is a transient Retry that the next push recovers from
          # on a fresh connection — not a "Not started" exception that tears the
          # run down and waits out a 5s reconnect.
          should_eventually(be_true, 4.seconds) { shovel.details_tuple[:confirmed] == 3 }
          shovel.details_tuple[:error].should be_nil
          shovel.terminate
        end
      end
    end

    it "retries once on a fresh connection when a kept-alive socket has gone stale" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        # Non-draining handler: the server closes the connection after every
        # response, so every other request lands on a dead keep-alive socket.
        server = HTTP::Server.new do |context|
          received.add(1)
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "sk_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "sk_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("sk_q1")
          4.times { |i| x.publish_confirm "m#{i}", "sk_q1" }
          spawn shovel.run
          # A stale keep-alive is only detectable by the next request dying on
          # it. That request is retried once on a fresh connection, so the
          # endpoint never saw a failure and the runner never sees a Retry.
          should_eventually(be_true, 3.seconds) { shovel.details_tuple[:confirmed] == 4 }
          shovel.details_tuple[:retried].should eq 0
          received.get.should eq 4
          shovel.terminate
        end
      end
    end

    it "reconnects after a transport failure in on-publish mode" do
      with_amqp_server do |s|
        # Non-draining handler: the server closes the connection after every
        # response, so every other request lands on a dead keep-alive socket.
        server = HTTP::Server.new do |context|
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        ack_mode = LavinMQ::Shovel::AckMode::OnPublish
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "op2_q1", ack_mode: ack_mode, direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"), ack_mode)
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "op2_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("op2_q1")
          3.times { |i| x.publish_confirm "m#{i}", "op2_q1" }
          spawn shovel.run
          # Crystal's HTTP::Client never drops a dead socket by itself for a
          # POST with a body; unless the destination closes it after the
          # failure, every later delivery fails on the same socket forever.
          should_eventually(be_true, 4.seconds) { shovel.details_tuple[:confirmed] == 3 }
          shovel.terminate
        end
      end
    end

    it "reconnects after a transport failure in no-ack mode" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          received.add(1)
          context.response.print "ok"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        ack_mode = LavinMQ::Shovel::AckMode::NoAck
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "na2_q1", ack_mode: ack_mode, direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"), ack_mode)
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "na2_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("na2_q1")
          4.times { |i| x.publish_confirm "m#{i}", "na2_q1" }
          spawn shovel.run
          # no-ack drops a message whose POST fails, but the failure must not
          # wedge the client: later messages still reach the endpoint.
          should_eventually(be_true, 3.seconds) { received.get >= 2 }
          shovel.terminate
        end
      end
    end

    it "aborts the shovel after repeated Abort responses from the HTTP destination (#5 Abort)" do
      with_amqp_server do |s|
        received = Atomic(Int32).new(0)
        server = HTTP::Server.new do |context|
          received.add(1)
          context.request.body.try &.skip_to_end
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
          # shovel gives up for an operator to resolve, rather than looping. That
          # is its own terminal state — distinct from the transient Error state
          # of a shovel that is about to reconnect — with the reason attached.
          should_eventually(be_true) { shovel.state.to_s == "Aborted" }
          d = shovel.details_tuple
          d[:error].to_s.should contain "destination unusable after 10 attempts"
          d[:aborted].should eq 10
          received.get.should eq 10
          should_eventually(eq 1) { q1.message_count }
        end
      end
    end

    it "retries a resumed shovel after it errored out on repeated Aborts" do
      with_amqp_server do |s|
        status = Atomic(Int32).new(404)
        server = HTTP::Server.new do |context|
          context.request.body.try &.skip_to_end
          context.response.status_code = status.get
          context.response.print "x"
          context
        end
        addr = server.bind_unused_port
        spawn server.listen

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "rs_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://#{addr}/"))
        shovel = LavinMQ::Shovel::Runner.new(source, dest, "rs_shovel", vhost)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("rs_q1")
          x.publish_confirm "deliver me eventually", "rs_q1"
          q1 = ch.queue("rs_q1")
          spawn shovel.run
          should_eventually(be_true) { shovel.state.aborted? }
          shovel.details_tuple[:error].to_s.should contain "unusable"
          # Aborting stops the run the way pause does: the source is closed
          # cleanly and the message stays on it for the operator.
          should_eventually(eq 1) { q1.message_count }
          # The operator fixes the endpoint and resumes the shovel straight from
          # Aborted. The new run must start with clean abort/failure counters
          # and actually try again.
          status.set(200)
          shovel.resume
          should_eventually(be_true) { shovel.details_tuple[:confirmed] == 1 }
          shovel.running?.should be_true
          shovel.terminate
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

  describe "HTTPDestination" do
    it "is not started once stopped" do
      # (port 1: start never connects, so no listener is needed)
      dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://127.0.0.1:1/"))
      dest.start
      dest.started?.should be_true
      dest.stop
      # A failover handler decides whether to (re)start a destination from
      # started?; a stopped one that still claims to be started is skipped.
      dest.started?.should be_false
    end
  end

  describe "HTTPDestination#classify" do
    it "rejects statuses that describe the message rather than the endpoint" do
      dest = LavinMQ::Shovel::HTTPDestination.new("spec", URI.parse("http://localhost/"))
      # Body size, Content-Type, uri_path and headers all come from the message,
      # so these say "this message is unacceptable", not "the endpoint is gone":
      # dead-letter the message and keep the shovel running.
      {400, 411, 413, 414, 415, 422, 431}.each do |code|
        dest.classify(HTTP::Client::Response.new(code)).should eq(LavinMQ::Shovel::Outcome::Reject), "status #{code}"
      end
      {301, 401, 403, 404, 405, 410, 418}.each do |code|
        dest.classify(HTTP::Client::Response.new(code)).should eq(LavinMQ::Shovel::Outcome::Abort), "status #{code}"
      end
      {408, 429, 500, 503}.each do |code|
        dest.classify(HTTP::Client::Response.new(code)).should eq(LavinMQ::Shovel::Outcome::Retry), "status #{code}"
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
          config = <<-JSON
            {
              "src-uri": "#{s.amqp_server.url}",
              "src-queue": "ct_q1",
              "dest-uri": "http://#{addr}/",
              "dest-timeout": 0.2
            }
            JSON
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

  describe "Runner#report" do
    it "ignores outcomes once the source is stopped" do
      with_amqp_server do |s|
        source = ShovelSpecHelpers::StoppedSource.new
        dest = ShovelSpecHelpers::StubDestination.new
        runner = LavinMQ::Shovel::Runner.new(source, dest, "rp_shovel", s.vhosts["/"])
        # Pause and terminate stop the source first, then the destination; the
        # destination's pending confirms are voided and come back as Retry. There
        # is nothing to settle (the source's channel close requeued them), so they
        # must not count as retries nor arm the delivery backoff.
        runner.report(1_u64, LavinMQ::Shovel::Outcome::Retry)
        runner.report(2_u64, LavinMQ::Shovel::Outcome::Confirmed)
        source.settlements.should be_empty
        runner.details_tuple[:retried].should eq 0
        runner.details_tuple[:confirmed].should eq 0
        runner.pending_backoff.should eq Time::Span.zero
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

    it "counts a burst of Retry outcomes as one failing round" do
      with_amqp_server do |s|
        source = ShovelSpecHelpers::PauseRaceSource.new
        dest = ShovelSpecHelpers::PauseRaceDestination.new
        runner = LavinMQ::Shovel::Runner.new(source, dest, "backoff", s.vhosts["/"])
        # Confirms arrive one per in-flight publish (up to prefetch), so a
        # reject-publish overflow nacks a whole window at once. That is one
        # failing round to back off from, not hundreds of them.
        10.times { |i| runner.report(i.to_u64 + 1, LavinMQ::Shovel::Outcome::Retry) }
        runner.details_tuple[:consecutive_failures].should eq 1
      end
    end

    it "waits once until the backoff deadline and clears it on a Confirmed" do
      with_amqp_server do |s|
        source = ShovelSpecHelpers::PauseRaceSource.new
        dest = ShovelSpecHelpers::PauseRaceDestination.new
        runner = LavinMQ::Shovel::Runner.new(source, dest, "backoff", s.vhosts["/"])
        runner.pending_backoff.should eq Time::Span.zero
        runner.report(1_u64, LavinMQ::Shovel::Outcome::Retry)
        runner.pending_backoff.should be <= 0.5.seconds
        runner.pending_backoff.should be > 0.3.seconds
        # A Retry inside the window belongs to the same failing round: it does
        # not push the deadline out or count another failure...
        runner.report(2_u64, LavinMQ::Shovel::Outcome::Retry)
        runner.details_tuple[:consecutive_failures].should eq 1
        runner.pending_backoff.should be <= 0.5.seconds
        # ...whereas the next round after the deadline doubles the window.
        sleep runner.pending_backoff
        runner.report(3_u64, LavinMQ::Shovel::Outcome::Retry)
        runner.details_tuple[:consecutive_failures].should eq 2
        runner.pending_backoff.should be > 0.5.seconds
        runner.pending_backoff.should be <= 1.second
        # Recovery is immediate: no leftover sleep before the next message.
        runner.report(4_u64, LavinMQ::Shovel::Outcome::Confirmed)
        runner.details_tuple[:consecutive_failures].should eq 0
        runner.pending_backoff.should eq Time::Span.zero
      end
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

    it "keeps rotating destinations while Aborts continue past a full cycle" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          a = ShovelSpecHelpers::FlakyStartDestination.new
          b = ShovelSpecHelpers::FlakyStartDestination.new
          parent = ShovelSpecHelpers::RecordingListener.new
          multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b] of LavinMQ::Shovel::Destination)
          multi.listener = parent
          multi.start
          4.times do |i|
            multi.report(i.to_u64 + 1, LavinMQ::Shovel::Outcome::Abort)
            multi.push(ShovelSpecHelpers.message(ch, i.to_u64 + 1)) # the redelivery
          end
          # Once every destination has aborted in a row the Abort propagates (so the
          # Runner's threshold applies), but each redelivery still goes to the next
          # destination rather than hammering the one that just aborted.
          parent.outcomes.map(&.last).should eq [
            LavinMQ::Shovel::Outcome::Retry,
            LavinMQ::Shovel::Outcome::Abort,
            LavinMQ::Shovel::Outcome::Abort,
            LavinMQ::Shovel::Outcome::Abort,
          ]
          {a.starts, b.starts}.should eq({3, 2})
        end
      end
    end

    it "carries out a failover on the next push, never inside the outcome callback" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          a = ShovelSpecHelpers::FlakyStartDestination.new
          b = ShovelSpecHelpers::FlakyStartDestination.new
          multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b] of LavinMQ::Shovel::Destination)
          multi.listener = ShovelSpecHelpers::RecordingListener.new
          multi.start
          multi.report(1_u64, LavinMQ::Shovel::Outcome::Abort)
          # report runs on the destination's confirm fiber for AMQP; stopping the
          # destination there deadlocks on its own connection close. The switch
          # waits for the Runner fiber, which is the one calling push.
          {a.stops, b.starts}.should eq({0, 0})
          multi.push(ShovelSpecHelpers.message(ch, 1_u64))
          {a.stops, b.starts, b.pushes}.should eq({1, 1, 1})
        end
      end
    end

    it "forwards outcomes voided by the failover itself without counting them" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          a = ShovelSpecHelpers::VoidingDestination.new
          b = ShovelSpecHelpers::FlakyStartDestination.new
          parent = ShovelSpecHelpers::RecordingListener.new
          multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b] of LavinMQ::Shovel::Destination)
          multi.listener = parent
          multi.start
          a.pending = [2_u64, 3_u64] # in flight on a when it aborts
          multi.report(1_u64, LavinMQ::Shovel::Outcome::Abort)
          multi.push(ShovelSpecHelpers.message(ch, 1_u64))
          # Stopping a voids its pending confirms, which come back as Retry: they
          # are forwarded so the source requeues them, but they are not a verdict
          # on b and must neither reset the abort streak nor request another
          # failover.
          parent.outcomes.should eq [
            {1_u64, LavinMQ::Shovel::Outcome::Retry},
            {2_u64, LavinMQ::Shovel::Outcome::Retry},
            {3_u64, LavinMQ::Shovel::Outcome::Retry},
          ]
          multi.report(4_u64, LavinMQ::Shovel::Outcome::Abort) # b aborts too: full cycle
          parent.outcomes.last.should eq({4_u64, LavinMQ::Shovel::Outcome::Abort})
          multi.push(ShovelSpecHelpers.message(ch, 4_u64))
          {a.starts, b.starts}.should eq({2, 1})
        end
      end
    end

    it "resets the abort streak when stopped and started again" do
      a = ShovelSpecHelpers::FlakyStartDestination.new
      b = ShovelSpecHelpers::FlakyStartDestination.new
      parent = ShovelSpecHelpers::RecordingListener.new
      multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b] of LavinMQ::Shovel::Destination)
      multi.listener = parent
      multi.start
      multi.report(1_u64, LavinMQ::Shovel::Outcome::Abort)
      multi.report(1_u64, LavinMQ::Shovel::Outcome::Abort) # full cycle: propagated
      multi.stop
      multi.start
      # A fresh run (pause/resume, reconnect) starts with a clean streak: the
      # first Abort fails over instead of propagating straight away.
      multi.report(2_u64, LavinMQ::Shovel::Outcome::Abort)
      parent.outcomes.last.should eq({2_u64, LavinMQ::Shovel::Outcome::Retry})
    end

    it "fails over after repeated Retry outcomes when more than one destination is configured" do
      a = ShovelSpecHelpers::FlakyStartDestination.new
      b = ShovelSpecHelpers::FlakyStartDestination.new
      parent = ShovelSpecHelpers::RecordingListener.new
      multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b] of LavinMQ::Shovel::Destination)
      multi.listener = parent
      multi.start
      # HTTP start never contacts the endpoint, so a dead host only ever shows
      # up as connection-refused Retries. A destination that keeps failing
      # transiently must not hold the shovel forever while a healthy one waits.
      with_amqp_server do |s|
        with_channel(s) do |ch|
          2.times do |i|
            multi.report(i.to_u64 + 1, LavinMQ::Shovel::Outcome::Retry)
            multi.push(ShovelSpecHelpers.message(ch, i.to_u64 + 1))
          end
          b.starts.should eq 0
          multi.report(3_u64, LavinMQ::Shovel::Outcome::Retry)
          multi.push(ShovelSpecHelpers.message(ch, 3_u64))
          {a.stops, b.starts}.should eq({1, 1})
          parent.outcomes.map(&.last).uniq!.should eq [LavinMQ::Shovel::Outcome::Retry]
        end
      end
    end

    it "does not fail over on Retry with a single destination" do
      a = ShovelSpecHelpers::FlakyStartDestination.new
      multi = LavinMQ::Shovel::MultiDestinationHandler.new([a] of LavinMQ::Shovel::Destination)
      multi.listener = ShovelSpecHelpers::RecordingListener.new
      multi.start
      with_amqp_server do |s|
        with_channel(s) do |ch|
          5.times do |i|
            multi.report(i.to_u64 + 1, LavinMQ::Shovel::Outcome::Retry)
            multi.push(ShovelSpecHelpers.message(ch, i.to_u64 + 1))
          end
        end
      end
      # Nothing to fail over to: restarting the same destination would only
      # churn its connection while the Runner backs off anyway.
      {a.starts, a.stops}.should eq({1, 0})
    end

    it "starts from the first destination again after a stop" do
      a = ShovelSpecHelpers::FlakyStartDestination.new
      b = ShovelSpecHelpers::FlakyStartDestination.new
      multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b] of LavinMQ::Shovel::Destination)
      multi.listener = ShovelSpecHelpers::RecordingListener.new
      multi.start
      multi.report(1_u64, LavinMQ::Shovel::Outcome::Abort) # fail over to b
      with_amqp_server do |s|
        with_channel(s) { |ch| multi.push(ShovelSpecHelpers.message(ch, 1_u64)) }
      end
      multi.stop
      multi.start
      # The list is an ordered preference: a restart (pause/resume, reconnect)
      # goes back to the primary rather than staying on whatever was active.
      {a.starts, b.starts}.should eq({2, 1})
    end

    it "starts from the first destination again after the runner reconnects" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "rc_q1", direct_user: s.users.direct_user)
        a = ShovelSpecHelpers::FlakyStartDestination.new
        b = ShovelSpecHelpers::FlakyStartDestination.new
        multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b] of LavinMQ::Shovel::Destination)
        shovel = LavinMQ::Shovel::Runner.new(source, multi, "rc_shovel", vhost, reconnect_delay: 50.milliseconds)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          ch.queue("rc_q1")
          x.publish_confirm "one", "rc_q1"
          spawn shovel.run
          should_eventually(eq 1) { a.pushes }
          multi.report(1_u64, LavinMQ::Shovel::Outcome::Abort) # a is unusable
          x.publish_confirm "two", "rc_q1"                     # the next push fails over to b
          should_eventually(eq 1) { b.starts }
          # The source connection drops. A reconnect is a fresh start: the
          # destination is stopped and the ordered preference applies again, so
          # the primary gets another chance rather than staying failed over.
          vhost.connections.each { |c| c.close("spec") if c.client_name.includes?("source") }
          should_eventually(eq 2) { a.starts }
          b.stops.should eq 1
          shovel.terminate
        end
      end
    end

    it "raises from start when no destination can be activated" do
      a = ShovelSpecHelpers::FlakyStartDestination.new(Socket::ConnectError.new("refused a"))
      b = ShovelSpecHelpers::FlakyStartDestination.new(Socket::ConnectError.new("refused b"))
      multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b] of LavinMQ::Shovel::Destination)
      # An unreachable destination is a connection error for the Runner's
      # reconnect loop, not a silent "started" that turns every push into an
      # Abort and errors the shovel out within milliseconds.
      expect_raises(Socket::ConnectError, "refused b") { multi.start }
      multi.started?.should be_false
    end

    it "tries every destination once at start when the first ones are down" do
      a = ShovelSpecHelpers::FlakyStartDestination.new(Socket::ConnectError.new("refused a"))
      b = ShovelSpecHelpers::FlakyStartDestination.new(Socket::ConnectError.new("refused b"))
      c = ShovelSpecHelpers::FlakyStartDestination.new
      multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b, c] of LavinMQ::Shovel::Destination)
      multi.start
      # The walk must not revisit a slot it already tried (and so skip c).
      {a.starts, b.starts, c.starts}.should eq({1, 1, 1})
      c.started?.should be_true
    end

    it "fails over past a destination that cannot start to the next one" do
      a = ShovelSpecHelpers::FlakyStartDestination.new
      b = ShovelSpecHelpers::FlakyStartDestination.new(Socket::ConnectError.new("refused b"))
      c = ShovelSpecHelpers::FlakyStartDestination.new
      parent = ShovelSpecHelpers::RecordingListener.new
      multi = LavinMQ::Shovel::MultiDestinationHandler.new([a, b, c] of LavinMQ::Shovel::Destination)
      multi.listener = parent
      multi.start
      multi.report(1_u64, LavinMQ::Shovel::Outcome::Abort) # a is unusable: fail over
      with_amqp_server do |s|
        with_channel(s) { |ch| multi.push(ShovelSpecHelpers.message(ch, 1_u64)) }
      end
      # b is down, so c must become active — not a restarted a.
      {a.starts, b.starts, c.starts}.should eq({1, 1, 1})
      c.started?.should be_true
      parent.outcomes.should eq [{1_u64, LavinMQ::Shovel::Outcome::Retry}]
    end

    it "makes the runner reconnect with backoff while the destination is unreachable" do
      with_amqp_server do |s|
        # Accepts and immediately drops connections: every destination start
        # fails, and the accept count shows the runner is still trying.
        attempts = Atomic(Int32).new(0)
        dead = TCPServer.new("127.0.0.1", 0)
        spawn do
          while client = dead.accept?
            attempts.add(1)
            client.close
          end
        end

        vhost = s.vhosts["/"]
        source = LavinMQ::Shovel::AMQPSource.new(
          "spec", [URI.parse(s.amqp_server.url)], "nd_q1", direct_user: s.users.direct_user)
        dest = LavinMQ::Shovel::AMQPDestination.new(
          "spec", URI.parse("amqp://127.0.0.1:#{dead.local_address.port}/"), "nd_q2", direct_user: s.users.direct_user)
        multi = LavinMQ::Shovel::MultiDestinationHandler.new([dest] of LavinMQ::Shovel::Destination)
        shovel = LavinMQ::Shovel::Runner.new(source, multi, "nd_shovel", vhost, reconnect_delay: 100.milliseconds)
        with_channel(s) do |ch|
          x = ch.exchange("", "direct", passive: true)
          q1 = ch.queue("nd_q1")
          x.publish_confirm "wait for me", "nd_q1"
          spawn shovel.run
          should_eventually(be_true) { shovel.state.error? }
          # A connection failure that keeps being retried — not the terminal
          # "destination unusable after 10 attempts" abort.
          should_eventually(be_true) { attempts.get >= 3 }
          shovel.details_tuple[:error].to_s.should_not contain "unusable"
          shovel.details_tuple[:aborted].should eq 0
          shovel.terminate
          should_eventually(eq 1) { q1.message_count }
        end
      ensure
        dead.try &.close
      end
    end
  end
end
