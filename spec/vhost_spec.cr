require "./spec_helper"

describe AvalancheMQ::Server do
  s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)

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
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    s.vhosts["test"]?.should_not be_nil
  end
end
