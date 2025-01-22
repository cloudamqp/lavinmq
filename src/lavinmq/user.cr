require "json"
require "./password"
require "./sortable_json"
require "./tag"

module LavinMQ
  class User
    include SortableJSON
    getter name, password, permissions
    property tags, plain_text_password
    alias Permissions = NamedTuple(config: Regex, read: Regex, write: Regex)

    @name : String
    @permissions = Hash(String, Permissions).new
    @password : Password? = nil
    @plain_text_password : String?
    @tags = Array(Tag).new

    def initialize(pull : JSON::PullParser)
      loc = pull.location
      name = hash = hash_algo = nil
      pull.read_object do |key|
        case key
        when "name"
          name = pull.read_string
        when "password_hash"
          hash = pull.read_string
        when "hashing_algorithm"
          hash_algo = pull.read_string
        when "permissions"
          parse_permissions(pull)
        when "tags"
          @tags = Tag.parse_list(pull.read_string)
        else nil
        end
      end
      raise JSON::ParseException.new("Missing json attribute: name", *loc) if name.nil?
      raise JSON::ParseException.new("Missing json attribute: password_hash", *loc) if hash.nil?
      @name = name
      @password = parse_password(hash, hash_algo, loc)
    end

    def self.create(name : String, password : String, hash_algorithm : String, tags : Array(Tag))
      pwd = hash_password(password, hash_algorithm)
      self.new(name, pwd, tags)
    end

    def self.hash_password(password, hash_algorithm)
      case hash_algorithm
      when /bcrypt$/i then BcryptPassword.create(password, cost: 4)
      when /sha256$/i then SHA256Password.create(password)
      when /sha512$/i then SHA512Password.create(password)
      when /md5$/i    then MD5Password.create(password)
      else                 raise UnknownHashAlgoritm.new(hash_algorithm)
      end
    end

    private def parse_password(hash, hash_algorithm, loc = nil)
      case hash_algorithm
      when /bcrypt$/i   then BcryptPassword.new(hash)
      when /sha256$/i   then SHA256Password.new(hash)
      when /sha512$/i   then SHA512Password.new(hash)
      when /md5$/i, nil then MD5Password.new(hash)
      else
        if loc
          raise JSON::ParseException.new("Unsupported hash algorithm", *loc)
        else
          raise UnknownHashAlgoritm.new(hash_algorithm)
        end
      end
    end

    def self.create_hidden_user(name)
      password = Random::Secure.urlsafe_base64(32)
      password_hash = hash_password(password, "sha256")
      user = self.new(name, password_hash, [Tag::Administrator])
      user.plain_text_password = password
      user
    end

    def initialize(@name, password_hash, hash_algorithm, @tags)
      update_password_hash(password_hash, hash_algorithm)
    end

    def initialize(@name, @password, @tags)
    end

    def hidden?
      UserStore.hidden?(@name)
    end

    def update_password_hash(password_hash, hash_algorithm)
      if password_hash.empty?
        @password = nil
        return
      end
      @password = parse_password(password_hash, hash_algorithm)
    end

    def update_password(password, hash_algorithm = "sha256")
      return if @password.try &.verify(password)
      @password = User.hash_password(password, hash_algorithm)
    end

    def details_tuple
      user_details.merge(permissions: @permissions)
    end

    def user_details
      {
        name:              @name,
        password_hash:     @password,
        hashing_algorithm: @password.try &.hash_algorithm,
        tags:              @tags.map(&.to_s.downcase).join(","),
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
        perm = permissions[vhost]?
        @acl_write_cache[cache_key] = perm ? perm_match?(perm[:write], name) : false
      end
      @acl_write_cache[cache_key]
    end

    @acl_read_cache = Hash({String, String}, Bool).new

    def can_read?(vhost, name)
      cache_key = {vhost, name}
      unless @acl_read_cache.has_key? cache_key
        perm = permissions[vhost]?
        @acl_read_cache[cache_key] = perm ? perm_match?(perm[:read], name) : false
      end
      @acl_read_cache[cache_key]
    end

    @acl_config_cache = Hash({String, String}, Bool).new

    def can_config?(vhost, name)
      cache_key = {vhost, name}
      unless @acl_config_cache.has_key? cache_key
        perm = permissions[vhost]?
        @acl_config_cache[cache_key] = perm ? perm_match?(perm[:config], name) : false
      end
      @acl_config_cache[cache_key]
    end

    def remove_queue_from_acl_caches(vhost, name)
      cache_key = {vhost, name}
      @acl_config_cache.delete(cache_key)
      @acl_read_cache.delete(cache_key)
      @acl_write_cache.delete(cache_key)
    end

    def can_impersonate?
      @tags.includes? Tag::Impersonator
    end

    def invalidate_acl_caches
      @acl_config_cache.clear
      @acl_read_cache.clear
      @acl_write_cache.clear
    end

    private def parse_permissions(pull)
      pull.read_object do |vhost|
        config = read = write = /^$/
        pull.read_object do |ikey|
          case ikey
          when "config" then config = Regex.from_json(pull)
          when "read"   then read = Regex.from_json(pull)
          when "write"  then write = Regex.from_json(pull)
          else               nil
          end
        end
        @permissions[vhost] = {config: config, read: read, write: write}
      end
    end

    private def perm_match?(perm, name)
      perm != /^$/ && perm != // && perm.matches? name
    end

    class UnknownHashAlgoritm < Exception; end
  end
end
