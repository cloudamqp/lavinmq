require "http/client"
require "json"
require "./auth_service"

module LavinMQ
  class HttpAuthHandler < AuthHandler
    def authenticate(username : String, password : String)
      payload = {
        "username" => username,
        "password" => password
      }.to_json

      success = ::HTTP::Client.post(@user_path,
        headers: ::HTTP::Headers{"Content-Type" => "application/json"},
        body: payload).success?
      if success
        "allow"
      else
        try_next(username, password)
      end
    end
  end
end
