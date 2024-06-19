require "./spec_helper"

class TestRouter
  include LavinMQ::HTTP::Router
end

def create_request(method, path)
  ::HTTP::Server::Context.new(
    ::HTTP::Request.new(method, path),
    ::HTTP::Server::Response.new(IO::Memory.new))
end

describe LavinMQ::HTTP::Router do
  it "is a HTTP::Handler" do
    TestRouter.new.should be_a ::HTTP::Handler
  end

  describe "#get" do
    it "registers route" do
      router = TestRouter.new
      router.get "a/:b" { |context, _params| context }
      router.@_routes.size.should eq 1
    end
  end

  describe "#call" do
    it "should route on method and path" do
      router = TestRouter.new
      routed = false
      router.get "a/:foo" do |context, params|
        params.should eq Hash(String, String){"foo" => "bar"}
        routed = true
        context
      end
      router.call(create_request("GET", "a/bar"))
      routed.should be_true
    end
  end
end

describe LavinMQ::HTTP::Router::StaticFragment do
  describe "#match?" do
    it "should be able to be empty string" do
      fragment = LavinMQ::HTTP::Router::StaticFragment.new("")
      params = Hash(String, String).new
      fragment.match?("", params).should be_true
    end
    it "should match on equal values" do
      fragment = LavinMQ::HTTP::Router::StaticFragment.new("foo")
      params = Hash(String, String).new
      fragment.match?("foo", params).should be_true
    end

    it "should not match on different values" do
      fragment = LavinMQ::HTTP::Router::StaticFragment.new("foo")
      params = Hash(String, String).new
      fragment.match?("bar", params).should be_false
    end
  end
end

describe LavinMQ::HTTP::Router::ParamFragment do
  describe "#match?" do
    it "should match and add value to params" do
      fragment = LavinMQ::HTTP::Router::ParamFragment.new("foo")
      params = Hash(String, String).new
      fragment.match?("bar", params).should be_true
      params.should eq Hash(String, String){"foo" => "bar"}
    end
  end
end

describe LavinMQ::HTTP::Router::RestFragment do
  describe "#match?" do
    it "should match and add value to params" do
      fragment = LavinMQ::HTTP::Router::RestFragment.new("foo")
      params = Hash(String, String).new
      fragment.match?("bar/baz/buz", params).should be_true
      params.should eq Hash(String, String){"foo" => "bar/baz/buz"}
    end
  end
end

describe LavinMQ::HTTP::Router::Fragments do
  describe ".create" do
    it "parses path and creates correct fragments" do
      fragments = LavinMQ::HTTP::Router::Fragments.create("a/:b/c/*d")
      expected = [
        LavinMQ::HTTP::Router::StaticFragment.new("a"),
        LavinMQ::HTTP::Router::ParamFragment.new("b"),
        LavinMQ::HTTP::Router::StaticFragment.new("c"),
        LavinMQ::HTTP::Router::RestFragment.new("d"),
      ]
      fragments.@fragments.should eq expected
    end
  end

  describe "#match?" do
    it "should handle path '/'" do
      fragments = LavinMQ::HTTP::Router::Fragments.create("/")
      params = LavinMQ::HTTP::Router::Params.new
      result = fragments.match?("/", params)
      result.should be_true
    end
    it "should match if all fragment matches" do
      fragments = LavinMQ::HTTP::Router::Fragments.create("a/:b/c")
      params = LavinMQ::HTTP::Router::Params.new
      result = fragments.match?("a/b/c", params)
      result.should be_true
      params.should eq Hash(String, String){"b" => "b"}
    end

    it "shouldn't match if one fragment doesn't match" do
      fragments = LavinMQ::HTTP::Router::Fragments.create("a/:b/c")
      params = LavinMQ::HTTP::Router::Params.new
      result = fragments.match?("a/b/d", params)
      result.should be_false
    end

    it "shouldn't match with too few fragments" do
      fragments = LavinMQ::HTTP::Router::Fragments.create("a/:b/c")
      params = LavinMQ::HTTP::Router::Params.new
      result = fragments.match?("a/b", params)
      result.should be_false
    end

    it "should collect all params" do
      fragments = LavinMQ::HTTP::Router::Fragments.create("a/:b/c/:d/*rest")
      params = LavinMQ::HTTP::Router::Params.new
      result = fragments.match?("/a/b/c/d/foo/bar", params)
      params.should eq LavinMQ::HTTP::Router::Params{"b" => "b", "d" => "d", "rest" => "foo/bar"}
      result.should be_true
    end
  end
end
