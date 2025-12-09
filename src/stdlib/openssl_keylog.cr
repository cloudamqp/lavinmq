require "openssl"

lib LibSSL
  {% if compare_versions(OPENSSL_VERSION, "1.1.1") >= 0 || compare_versions(LIBRESSL_VERSION, "3.4.1") >= 0 %}
    fun ssl_ctx_set_keylog_callback = SSL_CTX_set_keylog_callback(ctx : SSLContext, cb : (SSL, LibC::Char*) ->)
  {% end %}
  fun ssl_get_ssl_ctx = SSL_get_SSL_CTX(ssl : SSL) : SSLContext
end

class OpenSSL::SSL::Context::Server
  # Registry mapping SSL context handles to keylog files.
  # Required because OpenSSL's keylog callback doesn't support user data.
  @@keylog_files = Hash(LibSSL::SSLContext, File).new

  # Sets the SSL keylog file path for this context. When set, TLS session keys
  # will be logged to this file in NSS Key Log Format, which can be used to
  # decrypt TLS traffic.
  # Pass `nil` to disable key logging.
  # Requires OpenSSL 1.1.1+ or LibreSSL 3.4.1+.
  def keylog_file=(path : String?)
    {% if LibSSL.has_method?(:ssl_ctx_set_keylog_callback) %}
      @@keylog_files.delete(@handle).try &.close
      if path
        @@keylog_files[@handle] = File.open(path, "a").tap &.sync = true
        LibSSL.ssl_ctx_set_keylog_callback(@handle, ->(ssl : LibSSL::SSL, line : LibC::Char*) {
          ssl_ctx = LibSSL.ssl_get_ssl_ctx(ssl).as(LibSSL::SSLContext)
          if f = @@keylog_files[ssl_ctx]?
            len = LibC.strlen(line)
            str = String.new(len + 1) do |buf|
              buf.copy_from(line, len)
              buf[len] = '\n'.ord.to_u8
              {len + 1, len + 1}
            end
            f.print(str)
          end
        })
      else
        LibSSL.ssl_ctx_set_keylog_callback(@handle, nil)
      end
    {% else %}
      raise NotImplementedError.new("SSL keylog requires OpenSSL 1.1.1+ or LibreSSL 3.4.1+")
    {% end %}
  end
end
