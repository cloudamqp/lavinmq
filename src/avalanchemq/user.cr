require "crypto/bcrypt/password"
require "json"

module AvalancheMQ
  class User
    getter name, password
    def initialize(pull : JSON::PullParser)
      name = nil
      hash = nil
      pull.read_object do |key|
        case key
        when "name" 
          name = pull.read_string
        when "password_hash"
          hash = pull.read_string
        end
      end
      raise JSON::ParseException.new("Missing json attribute: name", *pull.location) if name.nil?
      raise JSON::ParseException.new("Missing json attribute: password_hash", *pull.location) if hash.nil?
      @name = name
      @password = Crypto::Bcrypt::Password.new(hash)
    end

    def initialize(@name : String, password : String? = nil, hash : String? = nil)
      @password =
        case
        when password
          Crypto::Bcrypt::Password.create(password)
        when hash
          Crypto::Bcrypt::Password.new(hash)
        else
          raise ArgumentError.new("Password or password_hash has to be given")
        end
    end

    def to_json(json)
      {
        name: @name,
        password_hash: @password.to_s
      }.to_json(json)
    end
  end
end
