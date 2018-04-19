module AvalancheMQ
  class User
    abstract class Password
      def self.create(password : String) : self
        salt = Random::Secure.random_bytes(32)
        dgst = OpenSSL::Digest.new(self.hash_algorithm)
        dgst.update salt
        dgst.update password
        self.new(salt, dgst.digest)
      end

      def initialize(@salt : Bytes, @hash : Bytes)
        buf = Bytes.new(@salt.bytesize + @hash.bytesize)
        @salt.copy_to(buf)
        @hash.copy_to(buf + @salt.bytesize)
        @raw_hash = hash_prefix + Base64.strict_encode(buf)
      end

      def initialize(@raw_hash : String)
        bytes = Base64.decode @raw_hash[hash_prefix.size..-1]
        @salt = bytes[0, 32]
        @hash = bytes + 32
      end

      def ==(password)
        dgst = OpenSSL::Digest.new(hash_algorithm)
        dgst.update @salt
        dgst.update password
        Crypto::Subtle.constant_time_compare(@hash, dgst.digest)
      end

      def to_s(io)
        io << @raw_hash
      end

      def inspect(io)
        to_s(io)
      end

      abstract def hash_algorithm : String
      abstract def hash_prefix : String
    end

    class SHA256Password < Password
      def self.hash_algorithm 
        "SHA256"
      end

      def hash_algorithm 
        "SHA256"
      end

      def hash_prefix
        "$5$"
      end
    end

    class MD5Password < Password
      def self.hash_algorithm 
        "MD5"
      end

      def hash_algorithm 
        "MD5"
      end

      def hash_prefix
        "$1$"
      end
    end
  end
end
