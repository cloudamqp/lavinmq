# Monkey-patch OpenSSL::SSL::Context::Server#on_server_name to copy
# settings that SSL_set_SSL_CTX does not propagate to the SSL connection:
# verify mode, verify depth, and client CA list.
# Without this, mTLS breaks when switching contexts via SNI.
# The connection keeps the listener's options: OpenSSL selects the TLS
# version and cipher before this callback runs, so option changes here
# could only clear post-handshake hardening like the renegotiation ban.

require "openssl"

lib LibSSL
  fun ssl_set_verify = SSL_set_verify(ssl : SSL, mode : LibC::Int, callback : Void*) : Void
  fun ssl_set_verify_depth = SSL_set_verify_depth(ssl : SSL, depth : LibC::Int) : Void
  fun ssl_ctx_get_verify_depth = SSL_CTX_get_verify_depth(ctx : SSLContext) : LibC::Int
  fun ssl_ctx_get_client_ca_list = SSL_CTX_get_client_CA_list(ctx : SSLContext) : Void*
  fun ssl_dup_ca_list = SSL_dup_CA_list(list : Void*) : Void*
  fun ssl_set_client_ca_list = SSL_set_client_CA_list(ssl : SSL, list : Void*) : Void
end

class OpenSSL::SSL::Context::Server
  @[Experimental]
  def on_server_name(&block : String -> OpenSSL::SSL::Context::Server?)
    c_callback = Proc(LibSSL::SSL, LibC::Int*, Void*, LibC::Int).new do |ssl, alert_ptr, arg|
      servername_ptr = LibSSL.ssl_get_servername(ssl, LibSSL::TLSExt::NAMETYPE_host_name)
      if servername_ptr.null?
        next LibSSL::SSL_TLSEXT_ERR_OK
      end

      begin
        hostname = String.new(servername_ptr)

        callback = Box(typeof(block)).unbox(arg)
        new_context = callback.call(hostname)

        if new_context
          new_ctx = new_context.to_unsafe
          LibSSL.ssl_set_ssl_ctx(ssl, new_ctx)

          # SSL_set_SSL_CTX does not copy these settings:
          verify_mode = LibSSL.ssl_ctx_get_verify_mode(new_ctx).to_i
          LibSSL.ssl_set_verify(ssl, verify_mode, nil)
          LibSSL.ssl_set_verify_depth(ssl, LibSSL.ssl_ctx_get_verify_depth(new_ctx))

          ca_list = LibSSL.ssl_ctx_get_client_ca_list(new_ctx)
          unless ca_list.null?
            LibSSL.ssl_set_client_ca_list(ssl, LibSSL.ssl_dup_ca_list(ca_list))
          end
        end

        LibSSL::SSL_TLSEXT_ERR_OK
      rescue
        alert_ptr.value = LibSSL::SSL_AD_INTERNAL_ERROR
        LibSSL::SSL_TLSEXT_ERR_ALERT_FATAL
      end
    end

    callback_box = Box.box(block)
    @sni_callback_box = callback_box

    LibSSL.ssl_ctx_callback_ctrl(@handle, LibSSL::SSL_CTRL_SET_TLSEXT_SERVERNAME_CB, c_callback.unsafe_as(Proc(Nil)))
    LibSSL.ssl_ctx_ctrl(@handle, LibSSL::SSL_CTRL_SET_TLSEXT_SERVERNAME_ARG, 0, callback_box)
  end
end
