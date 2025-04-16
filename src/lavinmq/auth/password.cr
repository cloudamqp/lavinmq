require "openssl"
require "crypto/bcrypt/password"

module LavinMQ
  class User
    module Password
      abstract def hash_algorithm : String
      abstract def verify(password : Bytes) : Bool

      def verify(password : String) : Bool
        verify(password.to_slice)
      end
    end

    class BcryptPassword < Crypto::Bcrypt::Password
      include Password

      def hash_algorithm : String
        "lavinmq_bcrypt"
      end

      def verify(password : Bytes) : Bool
        saltb = Crypto::Bcrypt::Base64.decode(salt, Crypto::Bcrypt::SALT_SIZE)
        passwordb = password.to_unsafe.to_slice(password.bytesize + 1).clone # include leading 0
        hashed_password = Crypto::Bcrypt.new(passwordb, saltb, cost)
        hashed_password_digest = Crypto::Bcrypt::Base64.encode(hashed_password.digest, hashed_password.digest.size - 1)
        Crypto::Subtle.constant_time_compare(@digest, hashed_password_digest)
      end
    end

    abstract class RabbitPassword
      include Password

      SALT_SIZE = 4

      def self.create(password : String) : self
        salt = Random::Secure.random_bytes(SALT_SIZE)
        dgst = OpenSSL::Digest.new(self.openssl_hash_algorithm)
        dgst.update salt
        dgst.update password
        self.new(salt, dgst.final)
      end

      def initialize(@salt : Bytes, @hash : Bytes)
        buf = Bytes.new(@salt.bytesize + @hash.bytesize)
        @salt.copy_to(buf)
        @hash.copy_to(buf + @salt.bytesize)
        @raw_hash = Base64.strict_encode(buf)
      end

      def initialize(raw_hash : String)
        # In earlier versions the hash type
        # was prefixed to the hash, like BCrypt always does,
        # but that's not compatible with other MQ servers when
        # exporting/importing definitions so here it's removed if
        # encountered
        if raw_hash.starts_with? hash_prefix
          @raw_hash = raw_hash[hash_prefix.size..-1]
        else
          @raw_hash = raw_hash
        end

        bytes = Base64.decode @raw_hash
        salt_size = bytes.size - digest_size
        @salt = bytes[0, salt_size]
        @hash = bytes + salt_size
        if @hash.bytesize != digest_size
          raise InvalidPasswordHash.new("Invalid digest size #{@hash.bytesize} for #{hash_algorithm}, expected #{digest_size}")
        end
      end

      def verify(password) : Bool
        dgst = OpenSSL::Digest.new(openssl_hash_algorithm)
        dgst.update @salt
        dgst.update password
        Crypto::Subtle.constant_time_compare(@hash, dgst.final)
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

      abstract def openssl_hash_algorithm : String
      abstract def hash_prefix : String
      abstract def digest_size : Int32
    end

    class SHA256Password < RabbitPassword
      def digest_size : Int32
        32
      end

      def hash_algorithm : String
        "rabbit_password_hashing_sha256"
      end

      def self.openssl_hash_algorithm : String
        "SHA256"
      end

      def openssl_hash_algorithm : String
        SHA256Password.openssl_hash_algorithm
      end

      def hash_prefix : String
        "$5$"
      end
    end

    class SHA512Password < RabbitPassword
      def digest_size : Int32
        64
      end

      def hash_algorithm : String
        "rabbit_password_hashing_sha512"
      end

      def self.openssl_hash_algorithm : String
        "SHA512"
      end

      def openssl_hash_algorithm : String
        SHA512Password.openssl_hash_algorithm
      end

      def hash_prefix : String
        "$6$"
      end
    end

    class MD5Password < RabbitPassword
      def digest_size : Int32
        16
      end

      def hash_algorithm : String
        "rabbit_password_hashing_md5"
      end

      def self.openssl_hash_algorithm : String
        "MD5"
      end

      def openssl_hash_algorithm : String
        MD5Password.openssl_hash_algorithm
      end

      def hash_prefix : String
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
