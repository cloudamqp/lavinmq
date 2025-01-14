require "./spec_helper"

describe LavinMQ::Cache do
  cache = LavinMQ::Cache(String, String).new(1.seconds)

  it "set key" do
    cache.set("key1", "allow").should eq "allow"
  end

  it "get key" do
    cache.set("keyget", "deny")
    cache.get?("keyget").should eq "deny"
  end

  it "invalid cache after 10 second" do
    cache.set("keyinvalid", "expired")
    sleep(2.seconds)
    cache.get?("keyinvalid").should be_nil
  end

  it "delete key" do
    cache.set("keydelete", "deleted")
    cache.delete("keydelete")
    cache.get?("keydelete").should be_nil
  end

  it "cleanup expired entry" do
    cache.set("clean1", "expired1")
    cache.set("clean2", "expired2")
    cache.set("clean3", "valid", 10.seconds)
    sleep(2.seconds)
    cache.get?("clean1").should be_nil
    cache.get?("clean2").should be_nil
    cache.get?("clean3").should eq "valid"
  end
end
