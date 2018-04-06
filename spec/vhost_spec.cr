require "./spec_helper"

describe AvalancheMQ::Server do
  s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)

  it "should be able to create vhosts" do
    s.create_vhost("test")
    s.vhosts.keys.includes?("test").should be_true
  end

  it "should be able to delete vhosts" do
    s.create_vhost("test")
    s.delete_vhost("test")
    s.vhosts.keys.includes?("test").should be_false
  end

  it "should be able to persist vhosts" do
    s.create_vhost("test")
    s.close
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    s.vhosts.keys.includes?("test").should be_true
  end
end
