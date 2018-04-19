require "crypto/bcrypt/password"
require "json"
require "./password"
require "./regex_to_json"

module AvalancheMQ
  class User
    getter name, password, permissions

    @name : String
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
      @password =
        case hash
        when /^\$2a\$/ then Crypto::Bcrypt::Password.new(hash)
        when /^\$1\$/ then MD5Password.new(hash)
        when /^\$5\$/ then SHA256Password.new(hash)
        else raise JSON::ParseException.new("Unsupported hash algorithm", *loc)
        end
    end

    def self.create(name : String, password : String, hash_algo : String)
      password =
        case hash_algo
        when "MD5" then MD5Password.create(password)
        when "SHA256" then SHA256Password.create(password)
        when "Bcrypt" then Crypto::Bcrypt::Password.create(password, cost: 4)
        else raise "Unknown password hash algorithm"
        end
      self.new(name, password)
    end

    def initialize(@name, @password)
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
