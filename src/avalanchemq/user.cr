require "crypto/bcrypt/password"
require "json"
require "./password"
require "./regex_to_json"

module AvalancheMQ
  enum Tag
    Administrator
    Monitoring
    Management
    PolicyMaker

    def to_json(json : JSON::Builder)
      to_s.downcase.to_json(json)
    end
  end

  class User
    getter name, password, permissions, hash_algorithm, tags
    setter tags

    @name : String
    @hash_algorithm : String
    @permissions = Hash(String, NamedTuple(config: Regex, read: Regex, write: Regex)).new
    @password = nil
    @tags = Array(Tag).new

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
              when "read"   then read = Regex.from_json(pull)
              when "write"  then write = Regex.from_json(pull)
              end
            end
            @permissions[vhost] = {config: config, read: read, write: write}
          end
        when "tags"
          @tags = pull.read_string.split(",").map { |t| Tag.parse?(t) }.compact
        end
      end
      raise JSON::ParseException.new("Missing json attribute: name", *loc) if name.nil?
      raise JSON::ParseException.new("Missing json attribute: password_hash", *loc) if hash.nil?
      @name = name
      @password =
        case hash
        when /^\$2a\$/ then Crypto::Bcrypt::Password.new(hash)
        when /^\$1\$/  then MD5Password.new(hash)
        when /^\$5\$/  then SHA256Password.new(hash)
        when /^\$6\$/  then SHA512Password.new(hash)
        when /^$/      then nil
        else                raise JSON::ParseException.new("Unsupported hash algorithm", *loc)
        end
      @hash_algorithm = hash_algorithm(hash)
    end

    def self.create(name : String, password : String, hash_algorithm : String, tags : Array(Tag))
      pwd = hash_password(password, hash_algorithm)
      self.new(name, pwd, tags)
    end

    def self.hash_password(password, hash_algorithm)
      case hash_algorithm
      when "MD5"    then MD5Password.create(password)
      when "SHA256" then SHA256Password.create(password)
      when "SHA512" then SHA512Password.create(password)
      when "Bcrypt" then Crypto::Bcrypt::Password.create(password, cost: 4)
      else               raise UnknownHashAlgoritm.new(hash_algorithm)
      end
    end

    def initialize(@name, password_hash, @hash_algorithm, @tags)
      update_password_hash(password_hash, @hash_algorithm)
    end

    def initialize(@name, @password, @tags)
      @hash_algorithm = hash_algorithm(@password.to_s)
    end

    def update_password_hash(password_hash, hash_algorithm)
      if password_hash.empty?
        @password = nil
        return
      end
      @password =
        case hash_algorithm
        when "MD5"    then MD5Password.new(password_hash)
        when "SHA256" then SHA256Password.new(password_hash)
        when "SHA512" then SHA512Password.new(password_hash)
        when "Bcrypt" then Crypto::Bcrypt::Password.new(password_hash)
        else               raise UnknownHashAlgoritm.new(hash_algorithm)
        end
      @hash_algorithm = hash_algorithm
    end

    def update_password(password, @hash_algorithm = "Bcrypt")
      @password = User.hash_password(password, hash_algorithm)
    end

    def to_json(json)
      user_details.merge(permissions: @permissions).to_json(json)
    end

    def user_details
      {
        name:              @name,
        password_hash:     @password.to_s,
        hashing_algorithm: @hash_algorithm,
        tags:              @tags.map { |t| t.to_s.downcase }.join(","),
      }
    end

    def permissions_details
      @permissions.map { |k, p| permissions_details(k, p) }
    end

    def permissions_details(vhost, p)
      {
        user:      @name,
        vhost:     vhost,
        configure: p[:config],
        read:      p[:read],
        write:     p[:write],
      }
    end

    @acl_write_cache = Hash({String, String}, Bool).new

    def can_write?(vhost, name)
      cache_key = {vhost, name}
      unless @acl_write_cache.has_key? cache_key
        perm = permissions[vhost][:write]
        @acl_write_cache[cache_key] = perm_match?(perm, name)
      end
      @acl_write_cache[cache_key]
    end

    @acl_read_cache = Hash({String, String}, Bool).new

    def can_read?(vhost, name)
      cache_key = {vhost, name}
      unless @acl_read_cache.has_key? cache_key
        perm = permissions[vhost][:read]
        @acl_read_cache[cache_key] = perm_match?(perm, name)
      end
      @acl_read_cache[cache_key]
    end

    @acl_config_cache = Hash({String, String}, Bool).new

    def can_config?(vhost, name)
      cache_key = {vhost, name}
      unless @acl_config_cache.has_key? cache_key
        perm = permissions[vhost][:config]
        @acl_config_cache[cache_key] = perm_match?(perm, name)
      end
      @acl_config_cache[cache_key]
    end

    def invalidate_acl_caches
      @acl_config_cache.clear
      @acl_read_cache.clear
      @acl_write_cache.clear
    end

    private def perm_match?(perm, name)
      perm != /^$/ && perm != // && !!perm.match name
    end

    private def hash_algorithm(hash)
      case hash
      when /^\$2a\$/ then "Bcrypt"
      when /^\$1\$/  then "MD5"
      when /^\$5\$/  then "SHA256"
      when /^\$6\$/  then "SHA512"
      when /^$/      then ""
      else                raise UnknownHashAlgoritm.new
      end
    end

    class UnknownHashAlgoritm < Exception; end
  end
end
