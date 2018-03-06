require "./spec_helper"

describe AMQPServer::TopicExchange do
  #it "matches exact rk" do
  #  x = AMQPServer::TopicExchange.new
  #  x.queues_matching("a.b").should eq(["q1", "q2"])
  #end

  it "matches star-wildcards" do
  end

  it "should not match with too many star-wildcards" do
  end

  it "should not match with too few star-wildcards" do
  end
end
