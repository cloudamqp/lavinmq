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
    it "is iterated correctly" do
      itr = LavinMQ::MQTT::StringTokenIterator.new(testdata[0], '/')
      res = Array(String).new
      while itr.next?
        if val = itr.next
          res << val
        end
      end
      itr.next?.should be_false
      res.should eq testdata[1]
    end
  end
end
