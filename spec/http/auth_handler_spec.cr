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
