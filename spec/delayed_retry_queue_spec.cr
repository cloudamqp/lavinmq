require "./spec_helper"
require "log/spec"

describe "Retry Queue" do
  describe "Scaffold" do
    it "should create internal retry queue when x-delayed-retry-min is set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 1000,
          })
          ch.queue("retry-test", args: args)
          retry_q = s.vhosts["/"].queue?("amq.retry-retry-test")
          retry_q.should_not be_nil
          retry_q.not_nil!.internal?.should be_true
        end
      end
    end

    it "should not create retry queue when x-delayed-retry-min is not set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delivery-limit" => 3})
          ch.queue("no-retry-test", args: args)
          s.vhosts["/"].queue?("amq.retry-no-retry-test").should be_nil
        end
      end
    end

    it "should reject an invalid x-delayed-retry-min" do
      with_amqp_server do |s|
        {-1, "bad"}.each do |value|
          with_channel(s) do |ch|
            expect_raises(AMQP::Client::Channel::ClosedException) do
              args = AMQP::Client::Arguments.new({
                "x-delivery-limit"    => 3,
                "x-delayed-retry-min" => value,
              })
              ch.queue("bad-retry", args: args)
            end
          end
        end
      end
    end

    it "should default x-delivery-limit to 20 when x-delayed-retry-min is set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1})
          q = ch.queue("retry-default-limit", args: args)
          ch.default_exchange.publish_confirm("default limit", q.name)

          21.times do
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          sleep 50.milliseconds
          q.get(no_ack: true).should be_nil
          s.vhosts["/"].queue("retry-default-limit").message_count.should eq 0
        end
      end
    end

    it "should not create duplicate retry queues on concurrent rejects" do
      with_amqp_server do |s|
        args = AMQP::Client::Arguments.new({
          "x-delivery-limit"    => 3,
          "x-delayed-retry-min" => 60_000,
        })
        with_channel(s) do |ch1|
          q = ch1.queue("retry-concurrent", args: args)
          q.publish_confirm "a"
          q.publish_confirm "b"
          with_channel(s) do |ch2|
            msg1 = wait_for { ch1.basic_get("retry-concurrent", no_ack: false) }
            msg2 = wait_for { ch2.basic_get("retry-concurrent", no_ack: false) }
            s.vhosts["/"].delete_queue("amq.retry-retry-concurrent")

            msg1.reject(requeue: true)
            msg2.reject(requeue: true)

            wait_for { s.vhosts["/"].queue?("amq.retry-retry-concurrent").try(&.message_count) == 2 }
            s.vhosts["/"].queue("retry-concurrent").message_count.should eq 0
          end
        end
      end
    end

    it "should recreate the retry queue if it was deleted" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 1000,
          })
          q = ch.queue("retry-recreate", args: args)
          s.vhosts["/"].delete_queue("amq.retry-retry-recreate")

          ch.default_exchange.publish_confirm("recreate test", q.name)
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          wait_for { s.vhosts["/"].queue?("amq.retry-retry-recreate").try(&.message_count) == 1 }
        end
      end
    end

    it "should ignore a client-supplied x-delivery-count when retry is not enabled" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({"x-delivery-limit" => 2})
          q = ch.queue("no-retry-forged-count", args: args)
          headers = AMQP::Client::Arguments.new({"x-delivery-count" => 1000})
          q.publish_confirm "forged", props: AMQP::Client::Properties.new(headers: headers)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          msg2 = wait_for { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "forged"
        end
      end
    end

    it "should count time spent in the retry queue against x-message-ttl" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-ttl-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 10,
            "x-delayed-retry-min"       => 500,
            "x-message-ttl"             => 300,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-ttl-dlq",
          })
          q = ch.queue("retry-ttl", args: args)
          q.publish_confirm "expire me"

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          dlq_msg = wait_for(timeout: 5.seconds) { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "expire me"
        end
      end
    end

    it "should delay a retried message again when the primary queue is full" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 100,
            "x-max-length"        => 1,
            "x-overflow"          => "reject-publish",
          })
          q = ch.queue("retry-overflow-redelay", args: args)
          q.publish_confirm "retry me"

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          q.publish_confirm "block"

          sleep 350.milliseconds
          s.vhosts["/"].queue("amq.retry-retry-overflow-redelay").message_count.should eq 1

          wait_for { q.get(no_ack: true) }.body_io.to_s.should eq "block"
          msg2 = wait_for(timeout: 5.seconds) { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "retry me"
        end
      end
    end

    it "should close the retry queue with the primary queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60_000,
          })
          q = ch.queue("retry-closed-primary", args: args)
          q.publish_confirm "survive"

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          wait_for { s.vhosts["/"].queue("amq.retry-retry-closed-primary").message_count == 1 }
          s.vhosts["/"].queue("retry-closed-primary").close

          retry_q = s.vhosts["/"].queue("amq.retry-retry-closed-primary")
          retry_q.closed?.should be_true
          retry_q.message_count.should eq 1
        end
      end
    end

    it "should requeue instantly if the retry queue store fails" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60_000,
          })
          q = ch.queue("retry-store-error", args: args)
          retry_q = s.vhosts["/"].queue("amq.retry-retry-store-error")
          FileUtils.rm_rf(retry_q.@msg_store.@msg_dir)

          body = "x" * (LavinMQ::Config.instance.segment_size + 1)
          q.publish_confirm body
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          msg2 = wait_for { q.get(no_ack: false) }
          msg2.body_io.to_s.should eq body
          retry_q.closed?.should be_true
          s.vhosts["/"].queue("retry-store-error").closed?.should be_false

          msg2.reject(requeue: true)
          wait_for { s.vhosts["/"].queue?("amq.retry-retry-store-error").try { |rq| !rq.closed? && rq.message_count == 1 } }
        end
      end
    end

    it "should purge delayed messages with the queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60_000,
          })
          q = ch.queue("retry-purge", args: args)
          q.publish_confirm "delayed"
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          wait_for { s.vhosts["/"].queue("amq.retry-retry-purge").message_count == 1 }

          Log.capture("lmq.*", :warn) do |logs|
            purged = ch.queue_purge("retry-purge")
            purged[:message_count].should eq 1
            logs.empty
          end
          s.vhosts["/"].queue("amq.retry-retry-purge").message_count.should eq 0
        end
      end
    end

    it "should refuse a multiplier above Int32 max" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delayed-retry-min"        => 1000,
            "x-delayed-retry-multiplier" => 3_000_000_000_i64,
          })
          expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED/) do
            ch.queue("retry-multiplier-overflow", args: args)
          end
        end
      end
    end

    it "should reject a queue name that leaves no room for the retry queue prefix" do
      with_amqp_server do |s|
        name = "q" * 246
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /too long/) do
            ch.queue(name, args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 1000}))
          end
        end
        s.vhosts["/"].queue?(name).should be_nil
        Dir.exists?(File.join(s.vhosts["/"].data_dir, Digest::SHA1.hexdigest(name))).should be_false
      end
    end

    it "should reject combining retry with message deduplication" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, /x-message-deduplication/) do
            args = AMQP::Client::Arguments.new({
              "x-delivery-limit"        => 3,
              "x-delayed-retry-min"     => 1000,
              "x-message-deduplication" => true,
            })
            ch.queue("retry-dedup", args: args)
          end
        end
      end
    end

    it "should clamp the retry delay instead of overflowing" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 100,
            "x-delayed-retry-min"        => 1,
            "x-delayed-retry-multiplier" => 4,
            "x-delayed-retry-max"        => 1,
          })
          q = ch.queue("retry-overflow", args: args)
          q.publish_confirm "overflow test"

          70.times do
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          ch.closed?.should be_false
          wait_for(timeout: 5.seconds) { q.get(no_ack: false) }.should_not be_nil
        end
      end
    end

    it "should give a retry-enabled dead letter queue its own retry budget" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq_args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60_000,
          })
          dlq = ch.queue("retry-chain-dlq", args: dlq_args)
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 1,
            "x-delayed-retry-min"       => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-chain-dlq",
          })
          q = ch.queue("retry-chain", args: args)
          q.publish_confirm "chain"

          2.times do
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          msg = wait_for(timeout: 5.seconds) { dlq.get(no_ack: false) }
          msg.properties.headers.try(&.["x-delivery-count"]?).should be_nil
          msg.reject(requeue: true)

          wait_for { s.vhosts["/"].queue("amq.retry-retry-chain-dlq").message_count == 1 }
        end
      end
    end

    it "should respect an explicit x-delivery-limit of 0" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 0,
            "x-delayed-retry-min" => 1,
          })
          q = ch.queue("retry-limit-zero", args: args)
          ch.default_exchange.publish_confirm("limit zero", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          sleep 50.milliseconds
          s.vhosts["/"].queue("retry-limit-zero").message_count.should eq 0
        end
      end
    end
  end

  describe "Basic retry" do
    it "should not retry on reject with requeue=false" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-no-requeue-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 3,
            "x-delayed-retry-min"       => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-no-requeue-dlq",
          })
          q = ch.queue("retry-no-requeue", args: args)
          ch.default_exchange.publish_confirm("reject test", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: false)

          dlq_msg = wait_for { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "reject test"
        end
      end
    end

    it "should preserve message body and properties through retry" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 1,
          })
          q = ch.queue("retry-props", args: args)
          props = AMQP::Client::Properties.new(
            content_type: "application/json",
            correlation_id: "abc-123",
            headers: AMQ::Protocol::Table.new({"x-custom" => "value"})
          )
          ch.default_exchange.publish_confirm("body", q.name, props: props)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          msg2 = wait_for { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "body"
          msg2.properties.content_type.should eq "application/json"
          msg2.properties.correlation_id.should eq "abc-123"
          headers = msg2.properties.headers.should_not be_nil
          headers["x-custom"].should eq "value"
        end
      end
    end

    it "should also trigger retry on nack(requeue=true)" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 200,
          })
          q = ch.queue("retry-nack-trigger", args: args)
          ch.default_exchange.publish_confirm("nack body", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.nack(requeue: true)
          start = Time.instant
          msg2 = wait_for(timeout: 5.seconds) { q.get(no_ack: true) }
          delay = Time.instant - start
          msg2.body_io.to_s.should eq "nack body"
          delay.should be >= 180.milliseconds
          delay.should be < 500.milliseconds
        end
      end
    end

    it "should not delay broker-initiated requeue on channel close" do
      with_amqp_server do |s|
        args = AMQP::Client::Arguments.new({
          "x-delivery-limit"    => 3,
          "x-delayed-retry-min" => 60000,
        })
        with_channel(s) do |ch|
          q = ch.queue("retry-close-bypass", args: args)
          ch.default_exchange.publish_confirm("close body", q.name)
          msg = wait_for { q.get(no_ack: false) }
          msg.body_io.to_s.should eq "close body"
        end
        # channel/connection closed without ack — unacked msg should requeue instantly, not into retry queue
        with_channel(s) do |ch|
          q = ch.queue("retry-close-bypass", args: args)
          start = Time.instant
          msg = wait_for(timeout: 2.seconds) { q.get(no_ack: true) }
          delay = Time.instant - start
          msg.body_io.to_s.should eq "close body"
          delay.should be < 500.milliseconds
          s.vhosts["/"].queue("amq.retry-retry-close-bypass").message_count.should eq 0
        end
      end
    end
  end

  describe "Exponential backoff" do
    it "should default to linear backoff when multiplier is omitted" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 5,
            "x-delayed-retry-min" => 200,
          })
          q = ch.queue("retry-linear", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          delays = [] of Time::Span
          3.times do
            start = Time.instant
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            delays << Time.instant - start
            msg.reject(requeue: true)
          end

          # Linear: delay = min × delivery_count → 200ms, 400ms, ...
          delays[1].should be >= 180.milliseconds
          delays[1].should be < 350.milliseconds
          delays[2].should be >= 380.milliseconds
          delays[2].should be < 600.milliseconds
        end
      end
    end

    it "should give constant delay when multiplier = 1" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 5,
            "x-delayed-retry-min"        => 200,
            "x-delayed-retry-multiplier" => 1,
          })
          q = ch.queue("retry-constant", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          delays = [] of Time::Span
          3.times do
            start = Time.instant
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            delays << Time.instant - start
            msg.reject(requeue: true)
          end

          # All retries should wait ~200ms (constant), not grow
          delays[1].should be >= 180.milliseconds
          delays[1].should be < 350.milliseconds
          delays[2].should be >= 180.milliseconds
          delays[2].should be < 350.milliseconds
        end
      end
    end

    it "should apply exponential delay when multiplier > 1" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 5,
            "x-delayed-retry-min"        => 200,
            "x-delayed-retry-multiplier" => 2,
          })
          q = ch.queue("retry-backoff", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          delays = [] of Time::Span
          3.times do
            start = Time.instant
            msg = wait_for(timeout: 10.seconds) { q.get(no_ack: false) }
            delays << Time.instant - start
            msg.reject(requeue: true)
          end

          delays[1].should be >= 180.milliseconds
          delays[1].should be < 600.milliseconds
          delays[2].should be >= 380.milliseconds
          delays[2].should be < 1000.milliseconds
        end
      end
    end

    it "should cap delay at x-delayed-retry-max" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 5,
            "x-delayed-retry-min"        => 100,
            "x-delayed-retry-multiplier" => 2,
            "x-delayed-retry-max"        => 250,
          })
          q = ch.queue("retry-cap", args: args)
          ch.default_exchange.publish_confirm("msg", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay1 = Time.instant - start
          delay1.should be >= 80.milliseconds
          delay1.should be < 300.milliseconds

          msg.reject(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay2 = Time.instant - start
          delay2.should be >= 180.milliseconds
          delay2.should be < 500.milliseconds

          msg.reject(requeue: true)
          start = Time.instant
          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
          delay3 = Time.instant - start
          delay3.should be >= 230.milliseconds
          delay3.should be < 600.milliseconds

          msg.ack
        end
      end
    end
  end

  describe "Delivery limit exhausted" do
    it "should dead-letter after delivery limit" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"          => 2,
            "x-delayed-retry-min"       => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "retry-dlq",
          })
          q = ch.queue("retry-exhaust-dlx", args: args)
          ch.default_exchange.publish_confirm("dlx test", q.name)

          3.times do
            msg = wait_for { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          dlq_msg = wait_for { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "dlx test"
          headers = dlq_msg.properties.headers.should_not be_nil
          headers["x-death"].should_not be_nil
        end
      end
    end

    it "should discard after delivery limit when no DLX" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 2,
            "x-delayed-retry-min" => 1,
          })
          q = ch.queue("retry-exhaust-discard", args: args)
          ch.default_exchange.publish_confirm("discard test", q.name)

          3.times do
            msg = wait_for { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          sleep 50.milliseconds
          q.get(no_ack: true).should be_nil
          s.vhosts["/"].queue("retry-exhaust-discard").message_count.should eq 0
        end
      end
    end

    it "should keep retrying at capped delay until x-delivery-limit ends it" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("retry-cap-dlq")
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"           => 4,
            "x-delayed-retry-min"        => 1,
            "x-delayed-retry-multiplier" => 2,
            "x-delayed-retry-max"        => 3,
            "x-dead-letter-exchange"     => "",
            "x-dead-letter-routing-key"  => "retry-cap-dlq",
          })
          q = ch.queue("retry-cap-pure", args: args)
          ch.default_exchange.publish_confirm("cap test", q.name)

          # Cap is a pure clamp; retries keep going at the capped delay until
          # x-delivery-limit terminates. With limit=4 the message is dead-lettered
          # on the 5th delivery attempt.
          5.times do
            msg = wait_for(timeout: 5.seconds) { q.get(no_ack: false) }
            msg.reject(requeue: true)
          end

          dlq_msg = wait_for { dlq.get(no_ack: true) }
          dlq_msg.body_io.to_s.should eq "cap test"
        end
      end
    end
  end

  describe "Cleanup" do
    it "should not error when primary queue is deleted while messages are in retry queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 60000,
          })
          q = ch.queue("retry-delete-pending", args: args)
          ch.default_exchange.publish_confirm("pending msg", q.name)

          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)

          sleep 10.milliseconds
          s.vhosts["/"].queue("amq.retry-retry-delete-pending").message_count.should eq 1

          q.delete
          s.vhosts["/"].queue?("amq.retry-retry-delete-pending").should be_nil
          s.vhosts["/"].queue?("retry-delete-pending").should be_nil
        end
      end
    end
  end

  describe "Durability" do
    it "should not persist the retry queue in the definitions file" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 1000,
          })
          ch.queue("retry-defs", durable: true, args: args)
          ch.queue("retry-defs-tmp", durable: true).delete
        end

        restart_server(s)

        defs = File.read(File.join(s.vhosts["/"].data_dir, "definitions.amqp"))
        defs.includes?("amq.retry-retry-defs").should be_false
        s.vhosts["/"].queue("amq.retry-retry-defs").should be_a(LavinMQ::AMQP::DelayedRetryQueue)
      end
    end

    it "should survive broker restart" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          args = AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 100,
          })
          q = ch.queue("retry-durable", durable: true, args: args)
          ch.default_exchange.publish_confirm("persist", q.name)
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          sleep 10.milliseconds
          s.vhosts["/"].queue("amq.retry-retry-durable").message_count.should eq 1
        end

        restart_server(s)

        s.vhosts["/"].queue?("amq.retry-retry-durable").should_not be_nil
        with_channel(s) do |ch|
          q = ch.queue("retry-durable", durable: true, args: AMQP::Client::Arguments.new({
            "x-delivery-limit"    => 3,
            "x-delayed-retry-min" => 100,
          }))
          msg = wait_for { q.get(no_ack: true) }
          msg.body_io.to_s.should eq "persist"
        end
      end
    end
  end

  describe "Policy" do
    it "should enable retries on an existing queue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-enable")
          vhost.add_policy("retry", "^retry-policy-enable$", "queues",
            {"delayed-retry-min" => JSON::Any.new(100_i64)}, 0_i8)
          wait_for { vhost.queue?("amq.retry-retry-policy-enable") }
          queue = vhost.queue("retry-policy-enable")
          queue.effective_policy_args.should contain "delayed-retry-min"
          queue.@delivery_limit.should eq 20

          q.publish_confirm "retry me"
          msg = wait_for { q.get(no_ack: false) }
          msg.reject(requeue: true)
          wait_for { vhost.queue("amq.retry-retry-policy-enable").message_count == 1 }
          queue.message_count.should eq 0

          msg2 = wait_for(timeout: 5.seconds) { q.get(no_ack: true) }
          msg2.body_io.to_s.should eq "retry me"
          msg2.properties.headers.not_nil!["x-delivery-count"].should eq 1
        end
      end
    end

    it "should apply the lower of argument and policy values" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          ch.queue("retry-policy-precedence", args: AMQP::Client::Arguments.new({
            "x-delayed-retry-min" => 500,
            "x-delayed-retry-max" => 10_000,
          }))
          queue = vhost.queue("retry-policy-precedence")
          vhost.add_policy("retry", "^retry-policy-precedence$", "queues", {
            "delayed-retry-min"        => JSON::Any.new(1000_i64),
            "delayed-retry-max"        => JSON::Any.new(5000_i64),
            "delayed-retry-multiplier" => JSON::Any.new(2_i64),
          }, 0_i8)
          wait_for { queue.policy }
          queue.@delayed_retry_min.should eq 500
          queue.@delayed_retry_max.should eq 5000
          queue.@delayed_retry_multiplier.should eq 2
          queue.effective_policy_args.should_not contain "delayed-retry-min"
          queue.effective_policy_args.should contain "delayed-retry-max"
          queue.details_tuple[:effective_arguments].should contain "x-delayed-retry-min"

          vhost.delete_policy("retry")
          wait_for { queue.policy.nil? }
          queue.@delayed_retry_min.should eq 500
          queue.@delayed_retry_max.should eq 10_000
          queue.@delayed_retry_multiplier.should be_nil
          vhost.queue("amq.retry-retry-policy-precedence").as(LavinMQ::AMQP::DelayedRetryQueue).draining?.should be_false
        end
      end
    end

    it "should apply updated values to subsequent retries only" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-update")
          vhost.add_policy("retry", "^retry-policy-update$", "queues",
            {"delayed-retry-min" => JSON::Any.new(60_000_i64)}, 0_i8)
          wait_for { vhost.queue?("amq.retry-retry-policy-update") }
          q.publish_confirm "slow"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { vhost.queue("amq.retry-retry-policy-update").message_count == 1 }

          vhost.add_policy("retry", "^retry-policy-update$", "queues",
            {"delayed-retry-min" => JSON::Any.new(50_i64)}, 0_i8)
          wait_for { vhost.queue("retry-policy-update").@delayed_retry_min == 50 }
          q.publish_confirm "fast"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)

          msg = wait_for(timeout: 5.seconds) { q.get(no_ack: true) }
          msg.body_io.to_s.should eq "fast"
          vhost.queue("amq.retry-retry-policy-update").message_count.should eq 1
        end
      end
    end

    it "should drain delayed messages when the policy is removed" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-drain")
          vhost.add_policy("retry", "^retry-policy-drain$", "queues",
            {"delayed-retry-min" => JSON::Any.new(500_i64)}, 0_i8)
          wait_for { vhost.queue?("amq.retry-retry-policy-drain") }
          q.publish_confirm "delayed"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          retry_q = vhost.queue("amq.retry-retry-policy-drain").as(LavinMQ::AMQP::DelayedRetryQueue)
          wait_for { retry_q.message_count == 1 }

          vhost.delete_policy("retry")
          queue = vhost.queue("retry-policy-drain")
          wait_for { queue.policy.nil? }
          queue.@delayed_retry_min.should be_nil
          queue.@delivery_limit.should be_nil
          retry_q.draining?.should be_true
          retry_q.message_count.should eq 1

          q.publish_confirm "instant"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { q.get(no_ack: true) }.body_io.to_s.should eq "instant"
          retry_q.message_count.should eq 1

          wait_for(timeout: 5.seconds) { q.get(no_ack: true) }.body_io.to_s.should eq "delayed"
          wait_for { vhost.queue?("amq.retry-retry-policy-drain").nil? }
          retry_q.@deleted.should be_true
        end
      end
    end

    it "should keep a draining retry queue when the policy is re-added" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-readd")
          definition = {"delayed-retry-min" => JSON::Any.new(60_000_i64)}
          vhost.add_policy("retry", "^retry-policy-readd$", "queues", definition, 0_i8)
          wait_for { vhost.queue?("amq.retry-retry-policy-readd") }
          q.publish_confirm "delayed"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          retry_q = vhost.queue("amq.retry-retry-policy-readd").as(LavinMQ::AMQP::DelayedRetryQueue)
          wait_for { retry_q.message_count == 1 }

          vhost.delete_policy("retry")
          wait_for { retry_q.draining? }
          vhost.add_policy("retry", "^retry-policy-readd$", "queues", definition, 0_i8)
          wait_for { !retry_q.draining? }
          vhost.queue("amq.retry-retry-policy-readd").should be retry_q
          retry_q.message_count.should eq 1
        end
      end
    end

    it "should delete an empty retry queue when the policy is removed" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          ch.queue("retry-policy-empty")
          vhost.add_policy("retry", "^retry-policy-empty$", "queues",
            {"delayed-retry-min" => JSON::Any.new(100_i64)}, 0_i8)
          wait_for { vhost.queue?("amq.retry-retry-policy-empty") }
          vhost.delete_policy("retry")
          wait_for { vhost.queue?("amq.retry-retry-policy-empty").nil? }
        end
      end
    end

    it "should delete a draining retry queue when the primary queue is purged" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-purge")
          vhost.add_policy("retry", "^retry-policy-purge$", "queues",
            {"delayed-retry-min" => JSON::Any.new(60_000_i64)}, 0_i8)
          wait_for { vhost.queue?("amq.retry-retry-policy-purge") }
          q.publish_confirm "delayed"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { vhost.queue("amq.retry-retry-policy-purge").message_count == 1 }
          vhost.delete_policy("retry")
          wait_for { vhost.queue("retry-policy-purge").policy.nil? }

          ch.queue_purge("retry-policy-purge")[:message_count].should eq 1
          wait_for { vhost.queue?("amq.retry-retry-policy-purge").nil? }
        end
      end
    end

    it "should skip retry policy keys on a queue with message deduplication" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-dedup", args: AMQP::Client::Arguments.new({"x-message-deduplication" => true}))
          vhost.add_policy("retry", "^retry-policy-dedup$", "queues", {
            "delayed-retry-min" => JSON::Any.new(100_i64),
            "delayed-retry-max" => JSON::Any.new(1000_i64),
            "max-length"        => JSON::Any.new(10_i64),
          }, 0_i8)
          queue = vhost.queue("retry-policy-dedup")
          wait_for { queue.policy }
          queue.effective_policy_args.should eq ["max-length"]
          queue.@delayed_retry_min.should be_nil
          queue.@delayed_retry_max.should be_nil
          queue.@delivery_limit.should be_nil
          vhost.queue?("amq.retry-retry-policy-dedup").should be_nil

          q.publish_confirm "m"
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { q.get(no_ack: true) }.body_io.to_s.should eq "m"
        end
      end
    end

    it "should skip retry policy keys when the retry queue name would be too long" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        name = "q" * 246
        with_channel(s) do |ch|
          ch.queue(name)
          vhost.add_policy("retry", "^q+$", "queues", {"delayed-retry-min" => JSON::Any.new(100_i64)}, 0_i8)
          queue = vhost.queue(name)
          wait_for { queue.policy }
          queue.effective_policy_args.should be_empty
          queue.@delayed_retry_min.should be_nil
        end
      end
    end

    it "should default the delivery limit and restore it when the policy is removed" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          ch.queue("retry-policy-limit")
          ch.queue("retry-policy-limit-arg", args: AMQP::Client::Arguments.new({"x-delivery-limit" => 3}))
          ch.queue("retry-policy-limit-retry-arg", args: AMQP::Client::Arguments.new({"x-delayed-retry-min" => 100}))
          plain = vhost.queue("retry-policy-limit")
          with_arg = vhost.queue("retry-policy-limit-arg")
          retry_arg = vhost.queue("retry-policy-limit-retry-arg")
          retry_arg.@delivery_limit.should eq 20

          vhost.add_policy("retry", "^retry-policy-limit", "queues",
            {"delayed-retry-min" => JSON::Any.new(100_i64)}, 0_i8)
          wait_for { plain.policy && with_arg.policy && retry_arg.policy }
          plain.@delivery_limit.should eq 20
          with_arg.@delivery_limit.should eq 3

          vhost.add_policy("retry", "^retry-policy-limit", "queues", {
            "delayed-retry-min" => JSON::Any.new(100_i64),
            "delivery-limit"    => JSON::Any.new(50_i64),
          }, 0_i8)
          wait_for { plain.@delivery_limit == 50 }
          with_arg.@delivery_limit.should eq 3
          retry_arg.@delivery_limit.should eq 50

          vhost.delete_policy("retry")
          wait_for { plain.policy.nil? && with_arg.policy.nil? && retry_arg.policy.nil? }
          plain.@delivery_limit.should be_nil
          with_arg.@delivery_limit.should eq 3
          retry_arg.@delivery_limit.should eq 20
        end
      end
    end

    it "should reattach the retry queue after restart" do
      with_amqp_server do |s|
        s.vhosts["/"].add_policy("retry", "^retry-policy-restart$", "queues",
          {"delayed-retry-min" => JSON::Any.new(60_000_i64)}, 0_i8)
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-restart", durable: true)
          wait_for { s.vhosts["/"].queue?("amq.retry-retry-policy-restart") }
          q.publish_confirm "persist", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { s.vhosts["/"].queue("amq.retry-retry-policy-restart").message_count == 1 }
        end

        restart_server(s)

        vhost = s.vhosts["/"]
        queue = vhost.queue("retry-policy-restart")
        wait_for { queue.policy }
        queue.@delayed_retry_min.should eq 60_000
        retry_q = vhost.queue("amq.retry-retry-policy-restart").as(LavinMQ::AMQP::DelayedRetryQueue)
        retry_q.should be queue.@delayed_retry_queue
        retry_q.draining?.should be_false
        retry_q.message_count.should eq 1
        vhost.queues.count(&.name.starts_with?("amq.retry-")).should eq 1
      end
    end

    it "should keep an empty retry queue across restart until the policy is applied" do
      with_amqp_server do |s|
        s.vhosts["/"].add_policy("retry", "^retry-policy-restart-empty$", "queues",
          {"delayed-retry-min" => JSON::Any.new(1_i64)}, 0_i8)
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-restart-empty", durable: true)
          wait_for { s.vhosts["/"].queue?("amq.retry-retry-policy-restart-empty") }
          q.publish_confirm "persist", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { q.get(no_ack: false) }.ack
          s.vhosts["/"].queue("amq.retry-retry-policy-restart-empty").message_count.should eq 0
        end

        restart_server(s)

        vhost = s.vhosts["/"]
        retry_q = vhost.queue("amq.retry-retry-policy-restart-empty").as(LavinMQ::AMQP::DelayedRetryQueue)
        retry_q.draining?.should be_false
        queue = vhost.queue("retry-policy-restart-empty")
        wait_for { queue.policy }
        queue.@delayed_retry_queue.should be retry_q
        retry_q.closed?.should be_false
      end
    end

    it "should keep draining after restart when the policy was removed" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.add_policy("retry", "^retry-policy-restart-drain$", "queues",
          {"delayed-retry-min" => JSON::Any.new(1_000_i64)}, 0_i8)
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-restart-drain", durable: true)
          wait_for { vhost.queue?("amq.retry-retry-policy-restart-drain") }
          q.publish_confirm "persist", props: AMQP::Client::Properties.new(delivery_mode: 2_u8)
          wait_for { q.get(no_ack: false) }.reject(requeue: true)
          wait_for { vhost.queue("amq.retry-retry-policy-restart-drain").message_count == 1 }
        end
        vhost.delete_policy("retry")
        wait_for { vhost.queue("retry-policy-restart-drain").policy.nil? }

        restart_server(s)

        vhost = s.vhosts["/"]
        vhost.queue("amq.retry-retry-policy-restart-drain").as(LavinMQ::AMQP::DelayedRetryQueue).draining?.should be_true
        with_channel(s) do |ch|
          q = ch.queue("retry-policy-restart-drain", durable: true)
          wait_for(timeout: 5.seconds) { q.get(no_ack: true) }.body_io.to_s.should eq "persist"
        end
        wait_for { vhost.queue?("amq.retry-retry-policy-restart-drain").nil? }
      end
    end
  end
end
