require "./spec_helper"

describe AvalancheMQ::Server do
  it "should be able to create vhosts" do
    s.vhosts.create("test")
    s.vhosts["test"]?.should_not be_nil
  ensure
    s.vhosts.delete("test")
  end

  it "should be able to delete vhosts" do
    s.vhosts.create("test")
    s.vhosts.delete("test")
    s.vhosts["test"]?.should be_nil
  ensure
    s.vhosts.delete("test")
  end

  it "should be able to persist vhosts" do
    s.vhosts.create("test")
    close_servers
    TestHelpers.setup
    s.vhosts["test"]?.should_not be_nil
  ensure
    s.vhosts.delete("test")
  end

  it "should be able to persist durable exchanges" do
    s.vhosts.create("test")
    v = s.vhosts["test"].not_nil!
    v.declare_exchange("e", "direct", true, false)
    close_servers
    TestHelpers.setup
    s.vhosts["test"].exchanges["e"].should_not be_nil
  ensure
    s.vhosts.delete("test")
  end

  it "should be able to persist durable queues" do
    s.vhosts.create("test")
    v = s.vhosts["test"].not_nil!
    v.declare_queue("q", true, false)
    close_servers
    TestHelpers.setup
    s.vhosts["test"].queues["q"].should_not be_nil
  ensure
    s.vhosts.delete("test")
  end

  it "should be able to persist bindings" do
    s.vhosts.create("test")
    v = s.vhosts["test"].not_nil!
    v.declare_exchange("e", "direct", true, false)
    v.declare_queue("q", true, false)
    s.vhosts["test"].bind_queue("q", "e", "q")

    close_servers
    TestHelpers.setup
    s.vhosts["test"].exchanges["e"].queue_bindings[{"q", nil}].size.should eq 1
  ensure
    s.vhosts.delete("test")
  end

  describe "auto add permissions" do
    it "should add permission to the user creating the vhost" do
      username = "test-user"
      user = s.users.create(username, "password", [AvalancheMQ::Tag::Administrator])
      vhost = "test-vhost"
      s.vhosts.create(vhost, user)
      p = user.permissions[vhost]
      p[:config].should eq /.*/
      p[:read].should eq /.*/
      p[:write].should eq /.*/
    ensure
      s.vhosts.delete(vhost)
      s.users.delete(username)
    end

    it "should auto add permission to the default user" do
      vhost = "test-vhost"
      s.vhosts.create(vhost)
      user = s.users.default_user
      p = user.permissions[vhost]
      p[:config].should eq /.*/
      p[:read].should eq /.*/
      p[:write].should eq /.*/
    ensure
      s.vhosts.delete(vhost)
    end
  end

  describe "Purge" do
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

        vhost = s.vhosts["/"]
        vhost.message_details[:messages].should eq 4

        vhost.reset!
        vhost.message_details[:messages].should eq 0
      end
    ensure
      s.vhosts["/"].delete_queue("purge_1")
      s.vhosts["/"].delete_queue("purge_2")
      s.vhosts["/"].delete_exchange("purge")
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

        vhost = s.vhosts["/"]
        vhost.reset!(true, "reset_spec")

        backup_dir = Path.new(vhost.data_dir, "..", "#{vhost.dir}_reset_spec").normalize
        Dir.exists?(backup_dir).should be_true
      end
    ensure
      s.vhosts["/"].delete_queue("purge_1")
      s.vhosts["/"].delete_queue("purge_2")
      s.vhosts["/"].delete_exchange("purge")
    end
  end
end
