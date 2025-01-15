require "http/client"
require "json"
require "../auth_handler"

module LavinMQ
  class HTTPAuthHandler < AuthHandler

    def initialize(successor : AuthHandler? = nil)
      @successor = successor
    end

    def authenticate(username : String, password : String)
      payload = {
        "username" => username,
        "password" => password
      }.to_json
      user_path = Config.instance.http_auth_url || ""
      success = ::HTTP::Client.post(user_path,
        headers: ::HTTP::Headers{"Content-Type" => "application/json"},
        body: payload).success?
      if success
        puts "HTTP authentication successful"
      else
        return @successor ? @successor.try &.authenticate(username, password, ) : nil
        try_next(username, password)
      end
    end
  end
end
