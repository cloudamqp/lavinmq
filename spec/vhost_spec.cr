require "./spec_helper"

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
      s.vhosts["test"].exchanges["e"].should_not be_nil
    end
  end

  it "should be able to persist durable delayed exchanges when type = x-delayed-message" do
    data_dir = ""
    with_amqp_server do |s|
      # This spec is to verify a fix where a server couldn't start again after a crash if
      # an delayed exchange had been declared by specifiying the type as "x-delayed-message".
      s.vhosts.create("test")
      v = s.vhosts["test"].not_nil!
      arguments = AMQ::Protocol::Table.new({"x-delayed-type": "direct"})
      v.declare_exchange("e", "x-delayed-message", true, false, arguments: arguments)

      # Start a new server with the same data dir as `Server` without stopping
      # `Server` first, because stopping would compact definitions and therefore "rewrite"
      data_dir = s.data_dir
    end
    # the definitions file. This is to simulate a start after a "crash".
    # If this succeeds we assume it worked...?
    LavinMQ::Server.new(data_dir)
  end

  it "should be able to persist durable queues" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      v = s.vhosts["test"].not_nil!
      v.declare_queue("q", true, false)
      s.restart
      s.vhosts["test"].queues["q"].should_not be_nil
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
      s.vhosts["test"].exchanges["e"].queue_bindings[{"q", nil}].size.should eq 1
    end
  end

  it "should not write bind frame to definition file for existing binding" do
    with_amqp_server do |s|
      s.vhosts.create("test")
      v = s.vhosts["test"].not_nil!
      v.declare_exchange("e", "direct", true, false)
      v.declare_queue("q", true, false)
      s.vhosts["test"].bind_queue("q", "e", "q")
      pos = v.@definitions_file.pos
      s.vhosts["test"].bind_queue("q", "e", "q")
      v.@definitions_file.pos.should eq pos
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
      pos = v.@definitions_file.pos
      s.vhosts["test"].unbind_queue("q", "e", "q")
      v.@definitions_file.pos.should eq pos
    end
  end

  it "should compact definitions during runtime" do
    with_amqp_server do |s|
      v = s.vhosts.create("test")
      (LavinMQ::Config.instance.max_deleted_definitions - 1).times do
        v.declare_queue("q", true, false)
        v.delete_queue("q")
      end
      file_size = v.@definitions_file.size
      v.declare_queue("q", true, false)
      v.delete_queue("q")
      v.@definitions_file.size.should be < file_size
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
  describe "Purge vhost" do
    it "should purge all queues in the vhost" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x = ch.exchange("purge", "direct")
          q1 = ch.queue("purge_1", durable: true)
          q2 = ch.queue("purge_1", durable: true)
          q1.bind(x.name, q1.name)
          q2.bind(x.name, q2.name)

          x.publish_confirm "test message 1.1", q1.name
          x.publish_confirm "test message 2.1", q1.name
          x.publish_confirm "test message 2.1", q2.name
          x.publish_confirm "test message 2.2", q2.name

          vhost = s.vhosts["/"]
          vhost.message_details[:messages].should eq 4

          vhost.purge_queues_and_close_consumers(false, "")
          vhost.message_details[:messages].should eq 0
        end
      end
    end

    it "should backup the folder on reset" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x = ch.exchange("purge", "direct")
          q1 = ch.queue("purge_1", durable: true)
          q2 = ch.queue("purge_1", durable: true)
          q1.bind(x.name, q1.name)
          q2.bind(x.name, q2.name)

          x.publish_confirm "test message 1.1", q1.name
          x.publish_confirm "test message 2.1", q1.name
          x.publish_confirm "test message 2.1", q2.name
          x.publish_confirm "test message 2.2", q2.name

          vhost = s.vhosts["/"]
          vhost.purge_queues_and_close_consumers(true, "reset_spec")

          backup_dir = Path.new(vhost.data_dir, "..", "#{vhost.dir}_reset_spec").normalize
          Dir.exists?(backup_dir).should be_true
        end
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
end
