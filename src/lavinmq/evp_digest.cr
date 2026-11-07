require "openssl"

module LavinMQ
  # Hashes on a bare EVP context freed right away. An OpenSSL::Digest (and
  # Digest::SHA1 and friends) registers a GC finalizer, and dead finalizable
  # objects make Boehm force collections, so code that runs per connection
  # or per queue hashes with this.
  module EVPDigest
    def self.digest(md : LibCrypto::EVP_MD, *parts : Bytes) : Bytes
      ctx = LibCrypto.evp_md_ctx_new
      raise OpenSSL::Digest::Error.new("EVP_MD_CTX_new") unless ctx
      begin
        hash = Bytes.new(LibCrypto.evp_md_size(md))
        ok = LibCrypto.evp_digestinit_ex(ctx, md, nil) == 1
        parts.each { |part| ok &&= LibCrypto.evp_digestupdate(ctx, part, part.bytesize) == 1 }
        ok &&= LibCrypto.evp_digestfinal_ex(ctx, hash, nil) == 1
        raise OpenSSL::Digest::Error.new("EVP_Digest") unless ok
        hash
      ensure
        LibCrypto.evp_md_ctx_free(ctx)
      end
    end

    def self.sha1_hexdigest(data : String) : String
      digest(LibCrypto.evp_sha1, data.to_slice).hexstring
    end
  end
end
