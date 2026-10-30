require "openssl"
require "crypto/bcrypt/password"

module LavinMQ
  module Auth
    module Password
      abstract def hash_algorithm : String
      abstract def verify(password : Bytes) : Bool

      def verify(password : String) : Bool
        verify(password.to_slice)
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
          new(salt, digest(salt, password.to_slice))
        end

        # Bare EVP context freed right away: an OpenSSL::Digest per login would register a GC
        # finalizer, which Boehm counts as 10 KB allocated when deciding to collect
        protected def self.digest(salt : Bytes, password : Bytes) : Bytes
          md = evp_md
          ctx = LibCrypto.evp_md_ctx_new
          raise OpenSSL::Digest::Error.new("EVP_MD_CTX_new") unless ctx
          begin
            hash = Bytes.new(LibCrypto.evp_md_size(md))
            ok = LibCrypto.evp_digestinit_ex(ctx, md, nil) == 1 &&
                 LibCrypto.evp_digestupdate(ctx, salt, salt.bytesize) == 1 &&
                 LibCrypto.evp_digestupdate(ctx, password, password.bytesize) == 1 &&
                 LibCrypto.evp_digestfinal_ex(ctx, hash, nil) == 1
            raise OpenSSL::Digest::Error.new("EVP_Digest") unless ok
            hash
          ensure
            LibCrypto.evp_md_ctx_free(ctx)
          end
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

          begin
            bytes = Base64.decode @raw_hash
          rescue ex : Base64::Error
            raise InvalidPasswordHash.new("Invalid password hash: not valid Base64 encoding (#{ex.message})")
          end
          if bytes.size < digest_size
            raise InvalidPasswordHash.new("Invalid password hash: decoded size #{bytes.size} bytes is smaller than required digest size #{digest_size} bytes for #{hash_algorithm}")
          end
          salt_size = bytes.size - digest_size
          @salt = bytes[0, salt_size]
          @hash = bytes + salt_size
          if @hash.bytesize != digest_size
            raise InvalidPasswordHash.new("Invalid digest size #{@hash.bytesize} for #{hash_algorithm}, expected #{digest_size}")
          end
        end

        def verify(password : Bytes) : Bool
          Crypto::Subtle.constant_time_compare(@hash, self.class.digest(@salt, password))
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

        def self.evp_md : LibCrypto::EVP_MD
          LibCrypto.evp_sha256
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

        def self.evp_md : LibCrypto::EVP_MD
          LibCrypto.evp_sha512
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

        def self.evp_md : LibCrypto::EVP_MD
          LibCrypto.evp_md5
        end

        def hash_prefix : String
          "$1$"
        end
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
