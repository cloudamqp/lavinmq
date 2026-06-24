require "./spec_helper"

class LavinMQ::DefinitionsStore
  def request_idle_compaction_for_spec
    @definitions_lock.synchronize do
      @last_definition_change = RoughTime.instant - WAL_COMPACT_IDLE - 1.second
    end
    @compact_requested.try_send nil
  end

  def compact_at_scheduled_with_wal_closed_for_spec : String | Exception
    result = Channel(String | Exception).new(1)
    @definitions_lock.synchronize do
      @last_definition_change = RoughTime.instant - WAL_COMPACT_IDLE - 1.second
      @definitions_file.close
      begin
        spawn do
          begin
            next_compact_at
            result.send "ok"
          rescue ex
            result.send ex
          end
        end
        100.times { Fiber.yield }
        if early_result = result.try_receive?
          return early_result
        end
      ensure
        @definitions_file = File.open(@definitions_file_path, "a+")
      end
    end
    result.receive
  end
end

describe LavinMQ::VHost do
  it "should be able to create vhosts" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      s.vhosts["test"]?.should_not be_nil
    end
  end

  it "should be able to delete vhosts" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      s.vhosts.delete("test")
      s.vhosts["test"]?.should be_nil
    end
  end

  it "should be able to persist vhosts" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      s.restart
      s.vhosts["test"]?.should_not be_nil
    end
  end

  it "should be able to persist durable exchanges" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      v = s.vhosts["test"].not_nil!
      v.declare_exchange("e", "direct", true, false)
      s.restart
      s.vhosts["test"].exchange("e").should_not be_nil
    end
  end

  it "should be able to persist durable delayed exchanges when type = x-delayed-message" do
    config = LavinMQ::Config.new
    with_amqp_server do |s|
      # This spec is to verify a fix where a server couldn't start again after a crash if
      # an delayed exchange had been declared by specifiying the type as "x-delayed-message".
      s.vhosts.create("test")
      v = s.vhosts["test"].not_nil!
      arguments = AMQ::Protocol::Table.new({"x-delayed-type": "direct"})
      v.declare_exchange("e", "x-delayed-message", true, false, arguments: arguments)

      # Start a new server with the same data dir as `Server` without stopping
      # `Server` first, because stopping would compact definitions and therefore "rewrite"
      config.data_dir = s.data_dir
    end
    # the definitions file. This is to simulate a start after a "crash".
    # If this succeeds we assume it worked...?
    LavinMQ::Server.new(config).close
  end

  it "should be able to persist durable queues" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      v = s.vhosts["test"].not_nil!
      v.declare_queue("q", true, false)
      s.restart
      s.vhosts["test"].queue("q").should_not be_nil
    end
  end

  it "should be able to persist bindings" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      v = s.vhosts["test"].not_nil!
      v.declare_exchange("e", "direct", true, false)
      v.declare_queue("q", true, false)
      s.vhosts["test"].bind_queue("q", "e", "q")
      s.restart
      s.vhosts["test"].exchange("e").bindings_details.first.destination.name.should eq "q"
    end
  end

  it "should replay definition wal records after json snapshots" do
    with_amqp_server do |s|
      v = s.vhosts.create("test")
      v.declare_exchange("e", "direct", true, false)
      v.declare_queue("q", true, false)
      v.bind_queue("q", "e", "q")

      File.exists?(File.join(v.data_dir, "exchanges.json")).should be_true
      File.exists?(File.join(v.data_dir, "queues.json")).should be_true
      File.exists?(File.join(v.data_dir, "bindings.json")).should be_true
      File.size(File.join(v.data_dir, "definitions.wal")).should be > 0

      s.restart
      v = s.vhosts["test"]
      v.exchange("e").should_not be_nil
      v.queue("q").should_not be_nil
      v.exchange("e").bindings_details.first.destination.name.should eq "q"
    end
  end

  it "tolerates a torn final definition wal record after a crash" do
    with_amqp_server do |s|
      v = s.vhosts.create("test")
      v.declare_exchange("e", "direct", true, false)
      v.declare_queue("q", true, false)
      v.bind_queue("q", "e", "q")
      File.size(File.join(v.data_dir, "definitions.wal")).should be > 0

      # Simulate a crash that left a half-written final record on disk.
      File.open(File.join(v.data_dir, "definitions.wal"), "a") do |f|
        f.print %({"op":"queue.declare","name":"tor)
      end

      s.restart
      v = s.vhosts["test"]
      v.queue("q").should_not be_nil
      v.queue?("tor").should be_nil
      v.exchange("e").bindings_details.first.destination.name.should eq "q"
    end
  end

  it "writes compact definition wal records" do
    with_amqp_server do |s|
      v = s.vhosts.create("test")
      v.declare_exchange("e", "direct", true, false)
      v.declare_queue("q", true, false)
      v.bind_queue("q", "e", "q")
      v.declare_queue("ttl", true, true, arguments: AMQ::Protocol::Table.new({"x-message-ttl" => 123}))

      records = [] of JSON::Any
      File.each_line(File.join(v.data_dir, "definitions.wal")) do |line|
        line = line.strip
        records << JSON.parse(line) unless line.empty?
      end

      exchange = records.find! { |r| r["op"].as_s == "exchange.declare" && r["name"].as_s == "e" }
      exchange.as_h.has_key?("durable").should be_false
      exchange.as_h.has_key?("auto_delete").should be_false
      exchange.as_h.has_key?("internal").should be_false
      exchange.as_h.has_key?("arguments").should be_false

      queue = records.find! { |r| r["op"].as_s == "queue.declare" && r["name"].as_s == "q" }
      queue.as_h.has_key?("durable").should be_false
      queue.as_h.has_key?("exclusive").should be_false
      queue.as_h.has_key?("auto_delete").should be_false
      queue.as_h.has_key?("arguments").should be_false

      binding = records.find! { |r| r["op"].as_s == "queue.bind" && r["queue"].as_s == "q" }
      binding.as_h.has_key?("arguments").should be_false

      auto_delete_queue = records.find! { |r| r["op"].as_s == "queue.declare" && r["name"].as_s == "ttl" }
      auto_delete_queue["auto_delete"].as_bool.should be_true
      auto_delete_queue["arguments"].as_h.has_key?("x-message-ttl").should be_true
    end
  end

  it "writes compact definition snapshots" do
    with_amqp_server do |s|
      v = s.vhosts.create("test")
      v.declare_exchange("plain", "direct", true, false)
      v.declare_exchange("internal", "direct", true, false, internal: true)
      v.declare_queue("plain", true, false)
      v.declare_queue("ttl", true, true, arguments: AMQ::Protocol::Table.new({"x-message-ttl" => 123}))
      v.bind_queue("plain", "plain", "plain")
      v.bind_queue("ttl", "internal", "rk", arguments: AMQ::Protocol::Table.new({"x-match" => "any"}))

      definitions = v.@definitions.not_nil!
      definitions.request_idle_compaction_for_spec
      wait_for { File.size(File.join(v.data_dir, "definitions.wal")) == 0 }

      exchanges = JSON.parse(File.read(File.join(v.data_dir, "exchanges.json"))).as_a
      plain_exchange = exchanges.find! { |e| e["name"].as_s == "plain" }
      plain_exchange.as_h.has_key?("durable").should be_false
      plain_exchange.as_h.has_key?("auto_delete").should be_false
      plain_exchange.as_h.has_key?("internal").should be_false
      plain_exchange.as_h.has_key?("arguments").should be_false

      internal_exchange = exchanges.find! { |e| e["name"].as_s == "internal" }
      internal_exchange["internal"].as_bool.should be_true

      queues = JSON.parse(File.read(File.join(v.data_dir, "queues.json"))).as_a
      plain_queue = queues.find! { |q| q["name"].as_s == "plain" }
      plain_queue.as_h.has_key?("durable").should be_false
      plain_queue.as_h.has_key?("exclusive").should be_false
      plain_queue.as_h.has_key?("auto_delete").should be_false
      plain_queue.as_h.has_key?("arguments").should be_false

      ttl_queue = queues.find! { |q| q["name"].as_s == "ttl" }
      ttl_queue["auto_delete"].as_bool.should be_true
      ttl_queue["arguments"].as_h.has_key?("x-message-ttl").should be_true

      bindings = JSON.parse(File.read(File.join(v.data_dir, "bindings.json"))).as_a
      plain_binding = bindings.find! { |b| b["destination"].as_s == "plain" }
      plain_binding.as_h.has_key?("arguments").should be_false

      binding_with_args = bindings.find! { |b| b["destination"].as_s == "ttl" }
      binding_with_args["arguments"].as_h.has_key?("x-match").should be_true

      s.restart
      v = s.vhosts["test"]
      v.exchange("plain").durable?.should be_true
      v.exchange("internal").internal?.should be_true
      v.queue("plain").durable?.should be_true
      v.queue("ttl").auto_delete?.should be_true
      v.queue("ttl").arguments.has_key?("x-message-ttl").should be_true
      v.exchange("internal").bindings_details.first.destination.name.should eq "ttl"
    end
  end

  it "should not restore stale queue bindings after queue delete and recreate" do
    with_amqp_server do |s|
      v = s.vhosts.create("test")
      v.declare_exchange("e", "direct", true, false)
      v.declare_queue("q", true, false)
      v.bind_queue("q", "e", "q")
      v.delete_queue("q")
      v.declare_queue("q", true, false)

      s.restart
      v = s.vhosts["test"]
      v.queue("q").should_not be_nil
      v.exchange("e").bindings_details.should be_empty
    end
  end

  it "should not write bind frame to definition file for existing binding" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      v = s.vhosts["test"].not_nil!
      v.declare_exchange("e", "direct", true, false)
      v.declare_queue("q", true, false)
      s.vhosts["test"].bind_queue("q", "e", "q")
      pos = v.@definitions.not_nil!.@definitions_file.pos
      s.vhosts["test"].bind_queue("q", "e", "q")
      v.@definitions.not_nil!.@definitions_file.pos.should eq pos
    end
  end

  it "should not write unbind frame to definition file for non-existing binding" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      v = s.vhosts["test"].not_nil!
      v.declare_exchange("e", "direct", true, false)
      v.declare_queue("q", true, false)
      s.vhosts["test"].bind_queue("q", "e", "q")
      s.vhosts["test"].unbind_queue("q", "e", "q")
      pos = v.@definitions.not_nil!.@definitions_file.pos
      s.vhosts["test"].unbind_queue("q", "e", "q")
      v.@definitions.not_nil!.@definitions_file.pos.should eq pos
    end
  end

  it "should compact definitions during runtime" do
    with_amqp_server do |s|
      v = s.vhosts.create("test")
      v.declare_queue("q", true, false)
      v.delete_queue("q")
      definitions = v.@definitions.not_nil!
      file_size = definitions.@definitions_file.size

      definitions.request_idle_compaction_for_spec
      wait_for { definitions.@definitions_file.size < file_size }

      File.exists?(File.join(v.data_dir, "exchanges.json")).should be_true
      File.exists?(File.join(v.data_dir, "queues.json")).should be_true
      File.exists?(File.join(v.data_dir, "bindings.json")).should be_true
      File.size(File.join(v.data_dir, "definitions.wal")).should eq 0
    end
  end

  it "snapshots definition WAL compaction scheduling state under the lock" do
    with_amqp_server do |s|
      v = s.vhosts.create("test")
      definitions = v.@definitions.not_nil!

      definitions.compact_at_scheduled_with_wal_closed_for_spec.should eq "ok"
    end
  end

  describe "auto add permissions" do
    it "should add permission to the user creating the vhost" do
      with_amqp_server do |s|
        username = "test-user"
        user = s.users.create(username, "password", [LavinMQ::Tag::Administrator])
        vhost = "test-vhost"
        s.vhosts.create(vhost, user)
        p = user.permissions[vhost]
        p[:config].should eq /.*/
        p[:read].should eq /.*/
        p[:write].should eq /.*/
      end
    end

    it "should auto add permission to the default user" do
      with_amqp_server do |s|
        vhost = "test-vhost"
        s.vhosts.create(vhost)
        user = s.users.default_user
        p = user.permissions[vhost]
        p[:config].should eq /.*/
        p[:read].should eq /.*/
        p[:write].should eq /.*/
      end
    end
  end

  it "can limit queues" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      vhost.max_queues = 1
      with_channel(s) do |ch|
        ch.queue
        expect_raises(AMQP::Client::Channel::ClosedException, /queue limit/) do
          ch.queue
        end
      end
      vhost.max_queues = -1
      with_channel(s) do |ch|
        ch.queue
      end
    end
  end

  it "can limit connections" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      vhost.max_connections = 1
      with_channel(s) do |_ch|
        expect_raises(AMQP::Client::Connection::ClosedException, /connection limit/) do
          with_channel(s) do |_ch2|
          end
        end
        vhost.max_connections = -1
        with_channel(s) do |_ch3|
        end
      end
    end
  end

  it "serializes concurrent saves so they don't race on the tmp file" do
    with_amqp_server do |s|
      store = s.vhosts
      # Concurrent vhost create/delete (e.g. under churn) all call save!, which
      # shares one vhosts.json.tmp path. Without serialization two saves race
      # and one's rename finds the tmp already moved by the other.
      failures = Atomic(Int32).new(0)
      WaitGroup.wait do |wg|
        40.times do
          wg.spawn do
            store.save!
          rescue
            failures.add(1)
          end
        end
      end
      failures.get.should eq 0
    end
  end
end
