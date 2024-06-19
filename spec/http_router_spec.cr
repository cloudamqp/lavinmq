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
    it "supports single param" do
      router = TestRouter.new
      routed = false
      router.get "/:foo" do |c, params|
        params.should eq Hash(String, String){"foo" => "bar"}
        routed = true
        c
      end
      router.call(create_request("GET", "/bar"))
      routed.should be_true
    end

    it "supports multiple params" do
      router = TestRouter.new
      routed = false
      router.get "/:foo/none/:bar" do |c, params|
        params.should eq Hash(String, String){"foo" => "bar", "bar" => "foo"}
        routed = true
        c
      end
      router.call(create_request("GET", "/bar/none/foo"))
      routed.should be_true
    end

    it "supports rest param" do
      router = TestRouter.new
      routed = false
      router.get "/a/*foo" do |c, params|
        params.should eq Hash(String, String){"foo" => "bar/baz"}
        routed = true
        c
      end
      router.call(create_request("GET", "/a/bar/baz"))
      routed.should be_true
    end

    it "should route on method and path" do
      router = TestRouter.new
      routed = false
      router.get "/a/:foo" do |context, params|
        params.should eq Hash(String, String){"foo" => "bar"}
        routed = true
        context
      end
      router.call(create_request("GET", "/a/bar"))
      routed.should be_true
    end
  end
end
