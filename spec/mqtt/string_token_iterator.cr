require "./spec_helper"
require "../src/myramq/string_token_iterator"

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

Spectator.describe MyraMQ::SubscriptionTree do
  sample strings do |testdata|
    it "is iterated correct" do
      itr = MyraMQ::StringTokenIterator.new(testdata[0], '/')
      res = Array(String).new
      while itr.next?
        val = itr.next
        expect(val).to_not be_nil
        res << val.not_nil!
      end
      expect(itr.next?).to be_false
      expect(res).to eq testdata[1]
    end
  end
end
