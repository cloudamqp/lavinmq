require "crypto/bcrypt/password"
require "json"
require "./password"
require "./regex_to_json"

module AvalancheMQ
  class User
    getter name, password, permissions, hash_algorithm

    @name : String
    @hash_algorithm : String
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
        when /^\$6\$/ then SHA512Password.new(hash)
        else raise JSON::ParseException.new("Unsupported hash algorithm", *loc)
        end
      @hash_algorithm = hash_algorithm(hash)
    end

    def self.create(name : String, password : String, hash_algo : String)
      password =
        case hash_algo
        when "MD5" then MD5Password.create(password)
        when "SHA256" then SHA256Password.create(password)
        when "SHA512" then SHA512Password.create(password)
        when "Bcrypt" then Crypto::Bcrypt::Password.create(password, cost: 4)
        else raise UnknownHashAlgoritm.new(hash_algo)
        end
      self.new(name, password)
    end

    def initialize(@name, password_hash, @hash_algorithm)
      @password =
        case @hash_algorithm
        when "MD5" then MD5Password.new(password_hash)
        when "SHA256" then SHA256Password.new(password_hash)
        when "SHA512" then SHA512Password.new(password_hash)
        when "Bcrypt" then Crypto::Bcrypt::Password.new(password_hash)
        else raise UnknownHashAlgoritm.new(@hash_algorithm)
        end
    end

    def initialize(@name, @password)
      @hash_algorithm = hash_algorithm(@password.to_s)
    end

    def to_json(json)
      {
        name: @name,
        password_hash: @password.to_s,
        permissions: @permissions,
        hashing_algorithm: @hash_algorithm
      }.to_json(json)
    end

    @acl_write_cache = Hash({String, String}, Bool).new
    def can_write?(vhost, name)
      cache_key = {vhost, name}
      unless @acl_write_cache.has_key? cache_key
        perm = permissions[vhost][:write]
        ok = perm != /^$/ && !!perm.match(name)
        @acl_write_cache[cache_key] = ok
      end
      @acl_write_cache[cache_key]
    end

    @acl_read_cache = Hash({String, String}, Bool).new
    def can_read?(vhost, name)
      cache_key = {vhost, name}
      unless @acl_read_cache.has_key? cache_key
        perm = permissions[vhost][:read]
        ok = perm != /^$/ && !!perm.match name
        @acl_read_cache[cache_key] = ok
      end
      @acl_read_cache[cache_key]
    end

    @acl_config_cache = Hash({String, String}, Bool).new
    def can_config?(vhost, name)
      cache_key = {vhost, name}
      unless @acl_config_cache.has_key? cache_key
        perm = permissions[vhost][:config]
        ok = perm != /^$/ && !!perm.match name
        @acl_config_cache[cache_key] = ok
      end
      @acl_config_cache[cache_key]
    end

    def invalidate_acl_caches
      @acl_config_cache.clear
      @acl_read_cache.clear
      @acl_write_cache.clear
    end

    private def hash_algorithm(hash)
      case hash
      when /^\$2a\$/ then "Bcrypt"
      when /^\$1\$/ then "MD5"
      when /^\$5\$/ then "SHA256"
      when /^\$6\$/ then "SHA512"
      else raise UnknownHashAlgoritm.new
      end
    end

    class UnknownHashAlgoritm < Exception; end
  end
end
