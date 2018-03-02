require "./spec_helper"

describe "RKMather" do
  describe "#topic" do
    #it "matches exact rk" do
    #  AMQPServer::Exchange::RKMather.topic("r.k", ["r.k"]).size.should eq 1
    #end

    #it "matches star-wildcards" do
    #  AMQPServer::Exchange::RKMather.topic("r.k", ["r.*", "*.k", "*.*"]).size.should eq 3
    #end

    #it "should not match with too many star-wildcards" do
    #  AMQPServer::Exchange::RKMather.topic("r.k", ["r.*.*", "*.*.k"]).size.should eq 0
    #end

    #it "should not match with too few star-wildcards" do
    #  AMQPServer::Exchange::RKMather.topic("r.k", ["*"]).size.should eq 0
    #end
  end
end
