require "openssl"
require "./gc"

# A digest frees its OpenSSL context in a GC finalizer. One that is only used
# to hash something once can free it right away and needs no finalizer.
class OpenSSL::Digest
  # Yields a digest for *name* and returns its final hash. The digest can't
  # be used after the block.
  def self.digest(name : String, & : self ->) : Bytes
    context = new(name)
    begin
      yield context
      context.final
    ensure
      context.free
    end
  end

  # Frees the context. The digest can't be used afterwards.
  def free : Nil
    return if @ctx.null?
    LibCrypto.evp_md_ctx_free(@ctx)
    @ctx = LibCrypto::EVP_MD_CTX.null
    @finished = true
    GC.remove_finalizer(self)
  end

  private def reset_impl : Nil
    raise FinalizedError.new("digest is freed") if @ctx.null?
    previous_def
  end
end

# `Digest::SHA1.hexdigest(data)` and the other class methods
module Digest::ClassMethods
  def digest(& : self ->) : Bytes
    context = new
    begin
      yield context
      context.final
    ensure
      context.free if context.responds_to?(:free)
    end
  end
end
