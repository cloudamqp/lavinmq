require "crypto/bcrypt/password"
require "json"

module AvalancheMQ
  class User
    getter name, password

    def initialize(pull : JSON::PullParser)
      loc = pull.location
      name = hash = nil
      pull.read_object do |key|
        case key
        when "name" 
          name = pull.read_string
        when "password_hash"
          hash = pull.read_string
        end
      end
      raise JSON::ParseException.new("Missing json attribute: name", *loc) if name.nil?
      raise JSON::ParseException.new("Missing json attribute: password_hash", *loc) if hash.nil?
      @name = name
      @password = Crypto::Bcrypt::Password.new(hash)
    end

    def initialize(@name : String, password : String)
      @password = Crypto::Bcrypt::Password.create(password, cost: 4)
    end

    def to_json(json)
      {
        name: @name,
        password_hash: @password.to_s
      }.to_json(json)
    end
  end
end
