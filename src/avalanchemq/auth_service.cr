module AvalancheMQ
  class AuthService
    def initialize(@user : User, @vhost : String)
    end

    @acl_write_cache = Hash(String, Bool).new
    def can_write?(name)
      unless @acl_write_cache.has_key? name
        perm = @user.permissions[@vhost][:write]
        ok = perm != /^$/ && !!perm.match(name)
        @acl_write_cache[name] = ok
      end
      @acl_write_cache[name]
    end

    @acl_read_cache = Hash(String, Bool).new
    def can_read?(name)
      unless @acl_read_cache.has_key? name
        perm = @user.permissions[@vhost][:read]
        ok = perm != /^$/ && !!perm.match name
        @acl_read_cache[name] = ok
      end
      @acl_read_cache[name]
    end

    @acl_config_cache = Hash(String, Bool).new
    def can_config?(name)
      unless @acl_config_cache.has_key? name
        perm = @user.permissions[@vhost][:config]
        ok = perm != /^$/ && !!perm.match name
        @acl_config_cache[name] = ok
      end
      @acl_config_cache[name]
    end
  end
end
