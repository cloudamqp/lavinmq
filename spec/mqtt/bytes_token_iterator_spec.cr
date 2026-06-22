require "./spec_helper"
require "../../src/lavinmq/mqtt/bytes_token_iterator"

describe LavinMQ::MQTT::BytesTokenIterator do
  # Must match StringTokenIterator's splitting semantics, including empty parts.
  [
    {"a", ["a"]},
    {"/", ["", ""]},
    {"a/", ["a", ""]},
    {"/a", ["", "a"]},
    {"a/b/c", ["a", "b", "c"]},
    {"a//c", ["a", "", "c"]},
    {"a//b/c/aa", ["a", "", "b", "c", "aa"]},
    {"long name here/and another long here",
     ["long name here", "and another long here"]},
  ].each do |input, expected|
    it "splits #{input.inspect} correctly" do
      itr = LavinMQ::MQTT::BytesTokenIterator.new(input.to_slice, '/')
      res = Array(String).new
      while itr.next?
        if val = itr.next
          res << String.new(val)
        end
      end
      itr.next?.should be_false
      res.should eq expected
    end
  end

  it "yields views into the backing buffer, never copies" do
    # Every token must point into the original buffer, not a fresh copy. This is
    # what keeps the subscription-tree match path allocation-free.
    buf = "aa/b/cccc/d/e".to_slice
    buf_start = buf.to_unsafe.address
    buf_end = buf_start + buf.size
    itr = LavinMQ::MQTT::BytesTokenIterator.new(buf, '/')
    tokens = [] of String
    while token = itr.next
      ptr = token.to_unsafe.address
      ptr.should be >= buf_start
      (ptr + token.size).should be <= buf_end
      tokens << String.new(token)
    end
    tokens.should eq ["aa", "b", "cccc", "d", "e"]
  end
end
