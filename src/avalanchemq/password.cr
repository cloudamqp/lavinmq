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
        unless @raw_hash.starts_with? hash_prefix
          @raw_hash = hash_prefix + @raw_hash
        end
        @salt = Bytes.new(0)
        @hash = Bytes.new(0)
        bytes = Base64.decode @raw_hash[hash_prefix.size..-1]
        case bytes.size
        when self.digest_size
          @hash = bytes
        when self.digest_size + 4
          @salt = bytes[0, 4]
          @hash = bytes + 4
        else raise InvalidPasswordHash.new("Invalid digest size #{bytes.size} for #{self.hash_algorithm}, raw size #{@raw_hash.size}")
        end
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

      def to_json(json)
        @raw_hash.to_json(json)
      end

      def inspect(io)
        to_s(io)
      end

      abstract def hash_algorithm : String
      abstract def hash_prefix : String
      abstract def digest_size : Int32
    end

    class SHA256Password < Password
      def digest_size
        32
      end

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

    class SHA512Password < Password
      def digest_size
        64
      end

      def self.hash_algorithm
        "SHA512"
      end

      def hash_algorithm
        "SHA512"
      end

      def hash_prefix
        "$6$"
      end
    end

    class MD5Password < Password
      def digest_size
        16
      end

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

    class InvalidPasswordHash < ArgumentError; end
  end
end

class Crypto::Bcrypt::Password
  def to_json(json : JSON::Builder)
    @raw_hash.to_json(json)
  end
end
