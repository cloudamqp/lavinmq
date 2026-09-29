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

    it "should uri decode params" do
      router = TestRouter.new
      routed = false
      router.get "/:foo" do |c, params|
        params.should eq Hash(String, String){"foo" => "hello world"}
        routed = true
        c
      end
      router.call(create_request("GET", "/hello%20world"))
      routed.should be_true
    end

    it "matches literal segments with percent-encoded unreserved chars" do
      router = TestRouter.new
      routed = false
      router.get "/a/mqtt.default" do |c, _params|
        routed = true
        c
      end
      router.call(create_request("GET", "/a/mqtt%2Edefault"))
      routed.should be_true
    end

    it "prefers an earlier literal route over a param route when the literal is percent-encoded" do
      router = TestRouter.new
      routed_to = nil
      router.get "/a/gc_stats" do |c, _params|
        routed_to = "literal"
        c
      end
      router.get "/a/:name" do |c, _params|
        routed_to = "param"
        c
      end
      router.call(create_request("GET", "/a/gc%5fstats"))
      routed_to.should eq "literal"
    end

    it "does not treat an encoded slash as a segment separator" do
      router = TestRouter.new
      routed = false
      router.get "/:vhost/:name" do |c, params|
        params.should eq Hash(String, String){"vhost" => "/", "name" => "a/b"}
        routed = true
        c
      end
      router.call(create_request("GET", "/%2F/a%2Fb"))
      routed.should be_true
    end

    it "decodes params exactly once" do
      router = TestRouter.new
      routed = false
      router.get "/:foo" do |c, params|
        params.should eq Hash(String, String){"foo" => "100%2E."}
        routed = true
        c
      end
      router.call(create_request("GET", "/100%252E%2E"))
      routed.should be_true
    end

    it "does not decode a bare percent sign together with the escapes after it" do
      router = TestRouter.new
      routed = false
      router.get "/:foo" do |c, params|
        params.should eq Hash(String, String){"foo" => "%2F"}
        routed = true
        c
      end
      router.call(create_request("GET", "/%%32%46"))
      routed.should be_true
    end

    it "routes paths with escapes that decode to invalid UTF-8" do
      router = TestRouter.new
      routed = false
      router.get "/:foo" do |c, params|
        params["foo"].valid_encoding?.should be_false
        routed = true
        c
      end
      router.call(create_request("GET", "/%FF"))
      routed.should be_true
    end
  end

  describe "#find_route" do
    it "registers route and finds it" do
      router = TestRouter.new
      router.get "/" { |context, _params| context }
      route = router.find_route("GET", "/")
      route.should_not be_nil
    end

    it "does not find unregistered route" do
      router = TestRouter.new
      route = router.find_route("GET", "/unregistered")
      route.should be_nil
    end
  end
end
