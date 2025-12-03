require "openssl"

lib LibSSL
  alias KeylogCallback = (SSL, LibC::Char*) ->

  {% if compare_versions(OPENSSL_VERSION, "1.1.1") >= 0 || compare_versions(LIBRESSL_VERSION, "3.4.1") >= 0 %}
    fun ssl_ctx_set_keylog_callback = SSL_CTX_set_keylog_callback(ctx : SSLContext, cb : KeylogCallback)
  {% end %}
end

class OpenSSL::SSL::Context::Server
  # Holds the keylog file reference to prevent GC collection
  # and manage lifecycle
  class_property keylog_file : File?

  # Sets the SSL keylog file path. When set, TLS session keys will be
  # logged to this file in NSS Key Log Format, which can be used by
  # Wireshark to decrypt TLS traffic.
  #
  # Pass nil or empty string to disable keylogging.
  def self.keylog_file=(path : String?)
    {% if LibSSL.has_method?(:ssl_ctx_set_keylog_callback) %}
      @@keylog_file.try &.close
      if path && !path.empty?
        @@keylog_file = File.open(path, "a").tap do |f|
          f.sync = true
        end
      else
        @@keylog_file = nil
      end
    {% else %}
      raise NotImplementedError.new("SSL keylog requires OpenSSL 1.1.1+ or LibreSSL 3.4.1+")
    {% end %}
  end

  # Enable keylogging on this context. The keylog file must be set
  # via `Server.keylog_file=` before calling this method.
  def enable_keylog
    {% if LibSSL.has_method?(:ssl_ctx_set_keylog_callback) %}
      if @@keylog_file
        LibSSL.ssl_ctx_set_keylog_callback(@handle, ->(ssl : LibSSL::SSL, line : LibC::Char*) {
          if file = OpenSSL::SSL::Context::Server.keylog_file
            file.puts String.new(line)
          end
        })
      else
        # Disable callback if no file is set
        LibSSL.ssl_ctx_set_keylog_callback(@handle, KeylogCallback.new(Pointer(Void).null, Pointer(Void).null))
      end
    {% end %}
  end

  # Disable keylogging on this context
  def disable_keylog
    {% if LibSSL.has_method?(:ssl_ctx_set_keylog_callback) %}
      LibSSL.ssl_ctx_set_keylog_callback(@handle, KeylogCallback.new(Pointer(Void).null, Pointer(Void).null))
    {% end %}
  end

  private alias KeylogCallback = LibSSL::KeylogCallback
end
