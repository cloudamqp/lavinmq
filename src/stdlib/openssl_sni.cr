# Monkey-patch OpenSSL::SSL::Context::Server with SNI callback support
# See: https://docs.openssl.org/3.5/man3/SSL_CTX_set_tlsext_servername_callback/

require "openssl"

lib LibSSL
  # SNI callback type: (SSL*, int*, void*) -> int
  # Returns SSL_TLSEXT_ERR_OK (0) on success
  alias SNICallback = (SSL, LibC::Int*, Void*) -> LibC::Int

  # SSL_CTX_set_tlsext_servername_callback is actually a macro that calls SSL_CTX_callback_ctrl
  # with SSL_CTRL_SET_TLSEXT_SERVERNAME_CB (53)
  SSL_CTRL_SET_TLSEXT_SERVERNAME_CB  = 53
  SSL_CTRL_SET_TLSEXT_SERVERNAME_ARG = 54

  # TLS alert codes
  SSL_AD_INTERNAL_ERROR = 80

  fun ssl_ctx_callback_ctrl = SSL_CTX_callback_ctrl(ctx : SSLContext, cmd : LibC::Int, fp : Proc(Void)) : LibC::Long
  fun ssl_set_ssl_ctx = SSL_set_SSL_CTX(ssl : SSL, ctx : SSLContext) : SSLContext
end

class OpenSSL::SSL::Context::Server
  # Stores the SNI callback Box to prevent garbage collection
  @sni_callback_box : Pointer(Void)?

  # Sets a Server Name Indication (SNI) callback.
  # The callback receives the hostname from the client and should return
  # an SSL::Context::Server configured for that hostname, or nil to use the default context.
  #
  # Example:
  # ```
  # context.set_sni_callback do |hostname|
  #   case hostname
  #   when "example.com"
  #     example_context
  #   when "api.example.com"
  #     api_context
  #   else
  #     nil # use default context
  #   end
  # end
  # ```
  def set_sni_callback(&block : String -> OpenSSL::SSL::Context::Server?)
    # Create a C callback that extracts the hostname and calls our Crystal block
    # IMPORTANT: This callback MUST NOT raise exceptions as they would unwind through
    # the LibSSL C stack, bypassing cleanup code and causing undefined behavior.
    c_callback = ->(ssl : LibSSL::SSL, alert_ptr : LibC::Int*, arg : Void*) : LibC::Int {
      begin
        # Get the server name from the SSL connection
        servername_ptr = LibSSL.ssl_get_servername(ssl, LibSSL::TLSExt::NAMETYPE_host_name)
        if servername_ptr.null?
          return LibSSL::SSL_TLSEXT_ERR_OK
        end

        hostname = String.new(servername_ptr)

        # Unbox the Crystal callback and call it
        callback = Box(Proc(String, OpenSSL::SSL::Context::Server?)).unbox(arg)
        new_context = callback.call(hostname)

        if new_context
          # Switch to the new SSL_CTX for this connection
          LibSSL.ssl_set_ssl_ctx(ssl, new_context.to_unsafe)
        end

        LibSSL::SSL_TLSEXT_ERR_OK
      rescue
        # NEVER let exceptions bubble through the C stack
        # Set alert code and return fatal error to abort the handshake
        alert_ptr.value = LibSSL::SSL_AD_INTERNAL_ERROR
        LibSSL::SSL_TLSEXT_ERR_ALERT_FATAL
      end
    }

    # Box the callback to pass to C
    callback_box = Box.box(block)
    @sni_callback_box = callback_box

    # Set the callback using SSL_CTX_callback_ctrl
    LibSSL.ssl_ctx_callback_ctrl(@handle, LibSSL::SSL_CTRL_SET_TLSEXT_SERVERNAME_CB, c_callback.unsafe_as(Proc(Void)))

    # Set the arg that will be passed to the callback
    LibSSL.ssl_ctx_ctrl(@handle, LibSSL::SSL_CTRL_SET_TLSEXT_SERVERNAME_ARG, 0, callback_box)
  end
end
