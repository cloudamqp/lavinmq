require "./spec_helper"

describe "poison-pill queue arguments" do
  it "parses x-nack-to-quarantine, x-quarantine-after-redeliveries, x-quarantine-target" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("pp-args", durable: true, args: AMQP::Client::Arguments.new({
          "x-nack-to-quarantine"            => true,
          "x-quarantine-after-redeliveries" => 5,
          "x-quarantine-target"             => "audit-bin",
        }))
        q = s.vhosts["/"].queue?("pp-args").as(LavinMQ::AMQP::Queue)
        q.nack_to_quarantine?.should be_true
        q.quarantine_after_redeliveries.should eq 5_i64
        q.quarantine_target.should eq "audit-bin"
        q.quarantine_action.should eq :move
      end
    end
  end

  it "parses x-quarantine-action as :move / :drop / :tee" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("pp-act-move", durable: true, args: AMQP::Client::Arguments.new({
          "x-nack-to-quarantine" => true,
          "x-quarantine-action"  => "move",
        }))
        s.vhosts["/"].queue?("pp-act-move").as(LavinMQ::AMQP::Queue).quarantine_action.should eq :move

        ch.queue("pp-act-drop", durable: true, args: AMQP::Client::Arguments.new({
          "x-nack-to-quarantine" => true,
          "x-quarantine-action"  => "drop",
        }))
        s.vhosts["/"].queue?("pp-act-drop").as(LavinMQ::AMQP::Queue).quarantine_action.should eq :drop

        ch.queue("pp-act-tee", durable: true, args: AMQP::Client::Arguments.new({
          "x-nack-to-quarantine" => true,
          "x-quarantine-target"  => "sink",
          "x-quarantine-action"  => "tee",
        }))
        s.vhosts["/"].queue?("pp-act-tee").as(LavinMQ::AMQP::Queue).quarantine_action.should eq :tee
      end
    end
  end

  it "rejects unknown x-quarantine-action values" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        expect_raises(AMQP::Client::Channel::ClosedException, /x-quarantine-action/) do
          ch.queue("pp-bad-action", durable: true, args: AMQP::Client::Arguments.new({
            "x-nack-to-quarantine" => true,
            "x-quarantine-action"  => "explode",
          }))
        end
      end
    end
  end

  it "defaults quarantine_action to :move when only redelivery threshold is set" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("pp-default", durable: true, args: AMQP::Client::Arguments.new({
          "x-quarantine-after-redeliveries" => 3,
        }))
        q = s.vhosts["/"].queue?("pp-default").as(LavinMQ::AMQP::Queue)
        q.quarantine_action.should eq :move
        q.quarantine_target.should be_nil
      end
    end
  end

  it "applies the same keys via a policy" do
    defs = {
      "nack-to-quarantine"            => JSON::Any.new(true),
      "quarantine-after-redeliveries" => JSON::Any.new(4_i64),
      "quarantine-target"             => JSON::Any.new("bin"),
      "quarantine-action"             => JSON::Any.new("tee"),
    } of String => JSON::Any
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("pp-policy", durable: true)
        s.vhosts["/"].add_policy("pp-pol", "^pp-policy$", "queues", defs, 10_i8)
        sleep 20.milliseconds
        q = s.vhosts["/"].queue?("pp-policy").as(LavinMQ::AMQP::Queue)
        q.nack_to_quarantine?.should be_true
        q.quarantine_after_redeliveries.should eq 4_i64
        q.quarantine_target.should eq "bin"
        q.quarantine_action.should eq :tee
        s.vhosts["/"].delete_policy("pp-pol")
      end
    end
  end
end
