require "http/client"
require "json"
require "./auth_service"

module LavinMQ
  class HttpAuthService < AuthenticationService
    def initialize(@method : String, @user_path : String, @whost_path : String, @resource_path : String, @topic_path : String)
    end

    def authorize?(username : String, password : String) : Bool
      payload = {
        "username" => username,
        "password" => password,
      }.to_json

      success = ::HTTP::Client.post(@user_path,
        headers: ::HTTP::Headers{"Content-Type" => "application/json"},
        body: payload).success?

      if success
        true
      else
        try_next(username, password)
      end
    end
  end
end
