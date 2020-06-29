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
end
