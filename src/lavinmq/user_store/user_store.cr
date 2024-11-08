require "../user"

module LavinMQ
  abstract class UserStore
    DIRECT_USER = "__direct"
    include Enumerable({String, User})
    Log = LavinMQ::Log.for "user_store"

    def self.hidden?(name)
      DIRECT_USER == name
    end

    abstract def create(name : String, password, tags = Array(Tag).new, save = true)
    abstract def add(name, password_hash, password_algorithm, tags = Array(Tag).new, save = true)
    abstract def add_permission(user, vhost, config, read, write)
    abstract def rm_permission(user, vhost)
    abstract def rm_vhost_permissions_for_all(vhost)
    abstract def delete(name, save = true) : User?
    abstract def default_user : User
    abstract def to_json(json : JSON::Builder)
    abstract def direct_user
    abstract def save!
  end
end
