require "./spec_helper"

describe AvalancheMQ::Server do
  s = amqp_server

  it "should be able to create vhosts" do
    s.vhosts.create("test")
    s.vhosts["test"]?.should_not be_nil
  end

  it "should be able to delete vhosts" do
    s.vhosts.create("test")
    s.vhosts.delete("test")
    s.vhosts["test"]?.should be_nil
  end

  it "should be able to persist vhosts" do
    s.vhosts.create("test")
    s.close
    s = amqp_server
    s.vhosts["test"]?.should_not be_nil
  end
end
