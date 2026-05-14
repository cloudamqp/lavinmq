require "./spec_helper"

private def filter_arg(json : String) : AMQP::Client::Arguments
  AMQP::Client::Arguments.new({"x-message-filter" => json})
end

describe "queue filter publish hook" do
  drop_rule = %({"clauses":[{"key":"x-test","op":"eq","value":"bad"}],"action":"drop"})

  it "drops matching messages and keeps non-matching" do
    defs = {"message-filter" => JSON.parse(drop_rule)} of String => JSON::Any
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("filter-drop")
        s.vhosts["/"].add_policy("p-drop", "^filter-drop$", "queues", defs, 10_i8)
        sleep 20.milliseconds
        3.times do |i|
          props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({"x-test" => "bad"}))
          q.publish_confirm "bad-#{i}", props: props
        end
        2.times do |i|
          props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({"x-test" => "good"}))
          q.publish_confirm "good-#{i}", props: props
        end
        ch.queue_declare("filter-drop", passive: true)[:message_count].should eq 2
      end
    end
  end

  it "skips filter for messages already carrying x-source-queue" do
    defs = {"message-filter" => JSON.parse(drop_rule)} of String => JSON::Any
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("filter-replay")
        s.vhosts["/"].add_policy("p-replay", "^filter-replay$", "queues", defs, 10_i8)
        sleep 20.milliseconds
        props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({
          "x-test"         => "bad",
          "x-source-queue" => "elsewhere",
        }))
        q.publish_confirm "replayed", props: props
        ch.queue_declare("filter-replay", passive: true)[:message_count].should eq 1
      end
    end
  end

  it "move_to publishes to target queue with x-source-* headers and removes from source" do
    rule = %({"clauses":[{"key":"x-tier","op":"eq","value":"free"}],"action":"move_to","target":"audit-bin","rule_id":"r1"})
    defs = {"message-filter" => JSON.parse(rule)} of String => JSON::Any
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("audit-bin")
        q = ch.queue("filter-move")
        s.vhosts["/"].add_policy("p-move", "^filter-move$", "queues", defs, 10_i8)
        sleep 20.milliseconds
        props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({"x-tier" => "free"}))
        q.publish_confirm "moved", props: props
        ch.queue_declare("filter-move", passive: true)[:message_count].should eq 0
        ch.queue_declare("audit-bin", passive: true)[:message_count].should eq 1
        msg = ch.basic_get("audit-bin", no_ack: true).not_nil!
        msg.body_io.to_s.should eq "moved"
        h = msg.properties.headers.not_nil!
        h["x-source-queue"].should eq "filter-move"
        h["x-source-exchange"].should eq ""
        h["x-source-routing-key"].should eq "filter-move"
        h["x-source-rule-id"].should eq "r1"
        h["x-source-timestamp"].should_not be_nil
        h["x-tier"].should eq "free"
      end
    end
  end

  it "move_to drops original when target queue missing" do
    rule = %({"clauses":[{"key":"x","op":"exists"}],"action":"move_to","target":"missing-bin"})
    defs = {"message-filter" => JSON.parse(rule)} of String => JSON::Any
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("filter-move-missing")
        s.vhosts["/"].add_policy("p-mm", "^filter-move-missing$", "queues", defs, 10_i8)
        sleep 20.milliseconds
        props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({"x" => "1"}))
        q.publish_confirm "x", props: props
        ch.queue_declare("filter-move-missing", passive: true)[:message_count].should eq 0
      end
    end
  end

  it "duplicate_to keeps original and publishes stamped copy" do
    rule = %({"clauses":[{"key":"x-audit","op":"exists"}],"action":"duplicate_to","target":"audit-sink","rule_id":"audit"})
    defs = {"message-filter" => JSON.parse(rule)} of String => JSON::Any
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("audit-sink")
        q = ch.queue("filter-dup")
        s.vhosts["/"].add_policy("p-dup", "^filter-dup$", "queues", defs, 10_i8)
        sleep 20.milliseconds
        props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({"x-audit" => "yes"}))
        q.publish_confirm "tee-me", props: props
        q.publish_confirm "plain"
        ch.queue_declare("filter-dup", passive: true)[:message_count].should eq 2
        ch.queue_declare("audit-sink", passive: true)[:message_count].should eq 1
        copy = ch.basic_get("audit-sink", no_ack: true).not_nil!
        copy.body_io.to_s.should eq "tee-me"
        copy.properties.headers.not_nil!["x-source-queue"].should eq "filter-dup"
        original = ch.basic_get("filter-dup", no_ack: true).not_nil!
        original.body_io.to_s.should eq "tee-me"
        original.properties.headers.not_nil!.has_key?("x-source-queue").should be_false
      end
    end
  end

  it "duplicate_to drops original when target queue missing" do
    rule = %({"clauses":[{"key":"x","op":"exists"}],"action":"duplicate_to","target":"missing-sink"})
    defs = {"message-filter" => JSON.parse(rule)} of String => JSON::Any
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("filter-dup-missing")
        s.vhosts["/"].add_policy("p-dm", "^filter-dup-missing$", "queues", defs, 10_i8)
        sleep 20.milliseconds
        props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({"x" => "1"}))
        q.publish_confirm "x", props: props
        ch.queue_declare("filter-dup-missing", passive: true)[:message_count].should eq 0
      end
    end
  end

  it "increments filter counters via message_stats" do
    rule = %({"clauses":[{"key":"x","op":"exists"}],"action":"drop"})
    defs = {"message-filter" => JSON.parse(rule)} of String => JSON::Any
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("filter-counter")
        s.vhosts["/"].add_policy("p-counter", "^filter-counter$", "queues", defs, 10_i8)
        sleep 20.milliseconds
        2.times do
          props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({"x" => "v"}))
          q.publish_confirm "m", props: props
        end
        queue = s.vhosts["/"].queue?("filter-counter").not_nil!.as(LavinMQ::AMQP::Queue)
        queue.filter_drop_count.should eq 2
        queue.filter_move_count.should eq 0
        queue.filter_duplicate_count.should eq 0
      end
    end
  end

  it "policy removal clears the filter" do
    defs = {"message-filter" => JSON.parse(drop_rule)} of String => JSON::Any
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("filter-clear")
        s.vhosts["/"].add_policy("p-clear", "^filter-clear$", "queues", defs, 10_i8)
        sleep 20.milliseconds
        queue = s.vhosts["/"].queue?("filter-clear").not_nil!.as(LavinMQ::AMQP::Queue)
        queue.filter.should_not be_nil
        s.vhosts["/"].delete_policy("p-clear")
        sleep 20.milliseconds
        queue.filter.should be_nil
      end
    end
  end
end
