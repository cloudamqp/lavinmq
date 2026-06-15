require "../coordinator"
require "../../config"

module LavinMQ
  module Clustering
    module VR
      # The Coordinator backed by Viewstamped Replication. With etcd gone, the
      # in-sync set and leader election are handled by the VR::Node + control
      # mesh + majority-quorum commit, so the only thing the Server still needs
      # from a Coordinator is the shared replication secret.
      #
      # The secret lives in `<data_dir>/.clustering_password` (mode 0600), the
      # canonical location. There is no shared store to mint one, so it must be
      # the same on every node: either drop the same file on each node, or set
      # `clustering_secret` in the config and the first start seeds the file from
      # it (after which the file wins, so rotate by replacing the file).
      class Coordinator < Clustering::Coordinator
        PASSWORD_FILE = ".clustering_password"

        def initialize(@config : Config)
        end

        def password : String
          path = File.join(@config.data_dir, PASSWORD_FILE)
          if File.file?(path)
            ensure_owner_only!(path)
            secret = File.read(path).strip
            raise Error.new("#{path} is empty") if secret.empty?
            return secret
          end
          # Not yet persisted: seed it from the configured secret, then the file
          # is authoritative on every subsequent start.
          secret = @config.clustering_secret
          if secret.nil? || secret.empty?
            raise Error.new("No clustering secret: create #{path} (same on all nodes) or set clustering_secret")
          end
          Dir.mkdir_p(@config.data_dir)
          File.write(path, secret, perm: File::Permissions.new(0o600))
          secret
        end

        # The secret file must be readable only by its owner. A group/world-
        # accessible secret is a real exposure, so refuse to start (fail fast)
        # rather than read it — mirroring how OpenSSH rejects loose key perms.
        private def ensure_owner_only!(path : String) : Nil
          mode = File.info(path).permissions.value
          if (mode & 0o077) != 0
            raise Error.new("#{path} is group/world-accessible (mode #{(mode & 0o777).to_s(8).rjust(3, '0')}); " \
                            "it holds the clustering secret. Restrict it: chmod 600 #{path}")
          end
        end
      end
    end
  end
end
