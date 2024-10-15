require "./spec_helper"
require "../../src/lavinmq/mqtt/string_token_iterator"

def strings
  [
    # { input, expected }
    {"a", ["a"]},
    {"/", ["", ""]},
    {"a/", ["a", ""]},
    {"/a", ["", "a"]},
    {"a/b/c", ["a", "b", "c"]},
    {"a//c", ["a", "", "c"]},
    {"a//b/c/aa", ["a", "", "b", "c", "aa"]},
    {"long name here/and another long here",
     ["long name here", "and another long here"]},
  ]
end

describe LavinMQ::MQTT::StringTokenIterator do
  strings.each do |testdata|
    it "is iterated correct" do
      itr = LavinMQ::MQTT::StringTokenIterator.new(testdata[0], '/')
      res = Array(String).new
      while itr.next?
        val = itr.next
        val.should_not be_nil
        res << val.not_nil!
      end
      itr.next?.should be_false
      res.should eq testdata[1]
    end
  end
end
