require "crypto/bcrypt/password"
require "json"

class Regex
  def to_json(io)
    self.source.to_json(io)
  end

  def self.from_json(pull)
    new(pull.read_string)
  end
end

module AvalancheMQ
  class User
    getter name, password, permissions

    @permissions = Hash(String, NamedTuple(config: Regex, read: Regex, write: Regex)).new

    def initialize(pull : JSON::PullParser)
      loc = pull.location
      name = hash = nil
      pull.read_object do |key|
        case key
        when "name" 
          name = pull.read_string
        when "password_hash"
          hash = pull.read_string
        when "permissions"
          pull.read_object do |key|
            vhost = key
            config = read = write = /^$/
            pull.read_object do |key|
              case key
              when "config" then config = Regex.from_json(pull)
              when "read" then read = Regex.from_json(pull)
              when "write" then write = Regex.from_json(pull)
              end
            end
            @permissions[vhost] = { config: config, read: read, write: write }
          end
        end
      end
      raise JSON::ParseException.new("Missing json attribute: name", *loc) if name.nil?
      raise JSON::ParseException.new("Missing json attribute: password_hash", *loc) if hash.nil?
      @name = name
      @password = Crypto::Bcrypt::Password.new(hash)
    end

    def initialize(@name : String, password : String)
      @password = Crypto::Bcrypt::Password.create(password, cost: 4)
      @permissions["/"] = { config: /.*/, read: /.*/, write: /.*/ }
    end

    def to_json(json)
      {
        name: @name,
        password_hash: @password.to_s,
        permissions: @permissions
      }.to_json(json)
    end
  end
end
