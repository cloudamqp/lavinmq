require "random/secure"
require "./durable_file"
require "./metadata"

module LavinMQ
  module Clustering
    class PasswordStore
      FILE_NAME = ".clustering_password"
      FILE_MODE = 0o600

      getter path : String

      def initialize(data_dir : String)
        @path = File.join(data_dir, FILE_NAME)
      end

      def password(seed : String? = nil) : String
        if File.exists?(@path)
          enforce_mode
          stored = File.read(@path).strip
          if seed && seed != stored
            raise Error.new("Configured clustering secret does not match #{@path}")
          end
          return stored
        end

        secret = seed || Random::Secure.base64(32)
        DurableFile.replace(@path, FILE_MODE) do |io|
          io.print secret
        end
        secret
      end

      private def enforce_mode : Nil
        info = File.info(@path)
        mode = info.permissions.value & 0o777
        return if mode == FILE_MODE
        raise Error.new("#{@path} must have mode 0600, got #{mode.to_s(8)}")
      end

      class Error < Exception; end
    end
  end
end
