require "./spec_helper"

describe LavinMQ::VHost do
  it "should be able to create vhosts" do
    Server.vhosts.create("test")
    Server.vhosts["test"]?.should_not be_nil
  end

  it "should be able to delete vhosts" do
    Server.vhosts.create("test")
    Server.vhosts.delete("test")
    Server.vhosts["test"]?.should be_nil
  end

  it "should be able to persist vhosts" do
    Server.vhosts.create("test")
    Server.restart
    Server.vhosts["test"]?.should_not be_nil
  end

  it "should be able to persist durable exchanges" do
    Server.vhosts.create("test")
    v = Server.vhosts["test"].not_nil!
    v.declare_exchange("e", "direct", true, false)
    Server.restart
    Server.vhosts["test"].exchanges["e"].should_not be_nil
  end

  it "should be able to persist durable queues" do
    Server.vhosts.create("test")
    v = Server.vhosts["test"].not_nil!
    v.declare_queue("q", true, false)
    Server.restart
    Server.vhosts["test"].queues["q"].should_not be_nil
  end

  it "should be able to persist bindings" do
    Server.vhosts.create("test")
    v = Server.vhosts["test"].not_nil!
    v.declare_exchange("e", "direct", true, false)
    v.declare_queue("q", true, false)
    Server.vhosts["test"].bind_queue("q", "e", "q")
    Server.restart
    Server.vhosts["test"].exchanges["e"].queue_bindings[{"q", nil}].size.should eq 1
  end

  it "should not write bind frame to definition file for existing binding" do
    Server.vhosts.create("test")
    v = Server.vhosts["test"].not_nil!
    v.declare_exchange("e", "direct", true, false)
    v.declare_queue("q", true, false)
    Server.vhosts["test"].bind_queue("q", "e", "q")
    pos = v.@definitions_file.pos
    Server.vhosts["test"].bind_queue("q", "e", "q")
    v.@definitions_file.pos.should eq pos
  end

  it "should not write unbind frame to definition file for non-existing binding" do
    Server.vhosts.create("test")
    v = Server.vhosts["test"].not_nil!
    v.declare_exchange("e", "direct", true, false)
    v.declare_queue("q", true, false)
    Server.vhosts["test"].bind_queue("q", "e", "q")
    Server.vhosts["test"].unbind_queue("q", "e", "q")
    pos = v.@definitions_file.pos
    Server.vhosts["test"].unbind_queue("q", "e", "q")
    v.@definitions_file.pos.should eq pos
  end

  it "should compact definitions during runtime" do
    Server.vhosts.create("test")
    v = Server.vhosts["test"].not_nil!
    (LavinMQ::VHost::DEFINITIONS_DIRT_COMPACT_THREASHOLD - 1).times do
      v.declare_queue("q", true, false)
      v.delete_queue("q")
    end
    v.@definitions_dirt_counter.should eq(LavinMQ::VHost::DEFINITIONS_DIRT_COMPACT_THREASHOLD - 1)
    definitions_file_pos_before = v.@definitions_file.pos
    v.declare_queue("q", true, false)
    v.delete_queue("q")
    wait_for(timeout: 1.second) { v.@definitions_dirt_counter.zero? }
    v.@definitions_file.pos.should be < definitions_file_pos_before
    v.@definitions_dirt_counter.should eq 0
  end

  describe "auto add permissions" do
    it "should add permission to the user creating the vhost" do
      username = "test-user"
      user = Server.users.create(username, "password", [LavinMQ::Tag::Administrator])
      vhost = "test-vhost"
      Server.vhosts.create(vhost, user)
      p = user.permissions[vhost]
      p[:config].should eq /.*/
      p[:read].should eq /.*/
      p[:write].should eq /.*/
    end

    it "should auto add permission to the default user" do
      vhost = "test-vhost"
      Server.vhosts.create(vhost)
      user = Server.users.default_user
      p = user.permissions[vhost]
      p[:config].should eq /.*/
      p[:read].should eq /.*/
      p[:write].should eq /.*/
    end
  end

  describe "Purge vhost" do
    it "should purge all queues in the vhost" do
      with_channel do |ch|
        x = ch.exchange("purge", "direct")
        q1 = ch.queue("purge_1", durable: true)
        q2 = ch.queue("purge_1", durable: true)
        q1.bind(x.name, q1.name)
        q2.bind(x.name, q2.name)

        x.publish_confirm "test message 1.1", q1.name
        x.publish_confirm "test message 2.1", q1.name
        x.publish_confirm "test message 2.1", q2.name
        x.publish_confirm "test message 2.2", q2.name

        vhost = Server.vhosts["/"]
        vhost.message_details[:messages].should eq 4

        vhost.purge_queues_and_close_consumers(false, "")
        vhost.message_details[:messages].should eq 0
      end
    end

    it "should backup the folder on reset" do
      with_channel do |ch|
        x = ch.exchange("purge", "direct")
        q1 = ch.queue("purge_1", durable: true)
        q2 = ch.queue("purge_1", durable: true)
        q1.bind(x.name, q1.name)
        q2.bind(x.name, q2.name)

        x.publish_confirm "test message 1.1", q1.name
        x.publish_confirm "test message 2.1", q1.name
        x.publish_confirm "test message 2.1", q2.name
        x.publish_confirm "test message 2.2", q2.name

        vhost = Server.vhosts["/"]
        vhost.purge_queues_and_close_consumers(true, "reset_spec")

        backup_dir = Path.new(vhost.data_dir, "..", "#{vhost.dir}_reset_spec").normalize
        Dir.exists?(backup_dir).should be_true
      end
    end
  end

  it "can limit queues" do
    vhost = Server.vhosts["/"]
    vhost.max_queues = 1
    with_channel do |ch|
      ch.queue
      expect_raises(AMQP::Client::Channel::ClosedException, /queue limit/) do
        ch.queue
      end
    end
    vhost.max_queues = -1
    with_channel do |ch|
      ch.queue
    end
  end

  it "can limit connections" do
    vhost = Server.vhosts["/"]
    vhost.max_connections = 1
    with_channel do |_ch|
      expect_raises(AMQP::Client::Connection::ClosedException, /connection limit/) do
        with_channel do |_ch2|
        end
      end
      vhost.max_connections = -1
      with_channel do |_ch3|
      end
    end
  end
end
