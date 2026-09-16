require "../spec_helper"

private def with_auth_handler(& : LavinMQ::HTTP::AuthHandler, LavinMQ::Auth::UserStore ->)
  users = LavinMQ::Auth::UserStore.new(LavinMQ::Config.instance.data_dir, nil)
  handler = LavinMQ::HTTP::AuthHandler.new(
    LavinMQ::Auth::LocalAuthenticator.new(users), users.direct_user,
    LavinMQ::Config.instance.control_unix_path)
  yield handler, users
end

private def request_context(headers = ::HTTP::Headers.new) : ::HTTP::Server::Context
  request = ::HTTP::Request.new("GET", "/api/overview", headers)
  request.remote_address = Socket::IPAddress.new("127.0.0.1", 1234)
  response = ::HTTP::Server::Response.new(IO::Memory.new)
  ::HTTP::Server::Context.new(request, response)
end

private def basic_auth_headers(credentials) : ::HTTP::Headers
  ::HTTP::Headers{"Authorization" => "Basic #{Base64.strict_encode(credentials)}"}
end

describe LavinMQ::HTTP::AuthHandler do
  describe "a request already authenticated by a previous handler" do
    it "authenticates as the user named by valid explicit credentials" do
      with_auth_handler do |handler, users|
        context = request_context(basic_auth_headers("guest:guest"))
        context.user = users.direct_user
        handler.call(context)
        context.user.try(&.name).should eq "guest"
      end
    end

    it "is rejected when explicit credentials are invalid" do
      with_auth_handler do |handler, users|
        context = request_context(basic_auth_headers("guest:wrong"))
        context.user = users.direct_user
        handler.call(context)
        context.user.should be_nil
      end
    end

    it "stays authenticated when no credentials are sent" do
      with_auth_handler do |handler, users|
        context = request_context
        context.user = users.direct_user
        handler.call(context)
        context.user.should eq users.direct_user
      end
    end

    it "authenticates via the Authorization header when the passwordless OAuth identity cookie accompanies it" do
      with_auth_handler do |handler, users|
        value = URI.encode_path_segment(Base64.strict_encode("sso-user:"))
        headers = basic_auth_headers("guest:guest")
        headers["Cookie"] = "m=|oauth:#{value}"
        context = request_context(headers)
        context.user = users.direct_user
        handler.call(context)
        context.user.try(&.name).should eq "guest"
      end
    end

    it "stays authenticated when only the passwordless OAuth identity cookie is sent" do
      with_auth_handler do |handler, users|
        value = URI.encode_path_segment(Base64.strict_encode("sso-user:"))
        context = request_context(::HTTP::Headers{"Cookie" => "m=|oauth:#{value}"})
        context.user = users.direct_user
        handler.call(context)
        context.user.should eq users.direct_user
      end
    end

    it "stays authenticated when the OAuth identity cookie username contains a colon" do
      with_auth_handler do |handler, users|
        value = URI.encode_path_segment(Base64.strict_encode("f:realm:alice:"))
        context = request_context(::HTTP::Headers{"Cookie" => "m=|oauth:#{value}"})
        context.user = users.direct_user
        handler.call(context)
        context.user.should eq users.direct_user
      end
    end
  end
end

private def with_scoped_user(& : LavinMQ::HTTP::AuthHandler, LavinMQ::Auth::UserStore ->)
  with_auth_handler do |handler, users|
    Dir.mkdir_p File.join(LavinMQ::Config.instance.data_dir, "tenant-users")
    users.load_vhost("tenant", File.join(LavinMQ::Config.instance.data_dir, "tenant-users"))
    users.create("alice", "global-pw", [LavinMQ::Tag::Management])
    users.create("alice", "scoped-pw", [LavinMQ::Tag::Management], vhost: "tenant")
    yield handler, users
  end
end

describe "vhost scoped users over HTTP" do
  it "authenticates a scoped user as vhost/name with Basic auth" do
    with_scoped_user do |handler, users|
      context = request_context(basic_auth_headers("tenant/alice:scoped-pw"))
      handler.call(context)
      context.user.should eq users["alice", "tenant"]
    end
  end

  it "authenticates the global user by plain name" do
    with_scoped_user do |handler, users|
      context = request_context(basic_auth_headers("alice:global-pw"))
      handler.call(context)
      context.user.should eq users["alice"]
      context = request_context(basic_auth_headers("alice:scoped-pw"))
      handler.call(context)
      context.user.should be_nil
      context = request_context(basic_auth_headers("other/alice:scoped-pw"))
      handler.call(context)
      context.user.should be_nil
    end
  end

  it "authenticates a scoped user from the login cookie" do
    with_scoped_user do |handler, users|
      auth = URI.encode_www_form(Base64.strict_encode("tenant/alice:scoped-pw"))
      context = request_context(::HTTP::Headers{"Cookie" => "m=|:#{auth}"})
      handler.call(context)
      context.user.should eq users["alice", "tenant"]
    end
  end

  it "prefers a global user whose name contains a slash" do
    with_scoped_user do |handler, users|
      global = users.create("tenant/alice", "slash-pw", [LavinMQ::Tag::Management])
      context = request_context(basic_auth_headers("tenant/alice:slash-pw"))
      handler.call(context)
      context.user.should eq global
      context = request_context(basic_auth_headers("tenant/alice:scoped-pw"))
      handler.call(context)
      context.user.should eq users["alice", "tenant"]
    end
  end

  it "splits at the last slash so users on the default vhost log in as //name" do
    with_auth_handler do |handler, users|
      Dir.mkdir_p File.join(LavinMQ::Config.instance.data_dir, "root-users")
      users.load_vhost("/", File.join(LavinMQ::Config.instance.data_dir, "root-users"))
      users.create("bob", "pw", [LavinMQ::Tag::Management], vhost: "/")
      context = request_context(basic_auth_headers("//bob:pw"))
      handler.call(context)
      context.user.should eq users["bob", "/"]
    end
  end
end
