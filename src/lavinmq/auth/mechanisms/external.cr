require "../../config"
require "./mechanism"

module LavinMQ
  module Auth
    module Mechanisms
      # SASL EXTERNAL: derives the identity from the client's x509 certificate,
      # based on the configured `external_auth_login_from`. Any response sent by
      # the client is ignored.
      #
      # Only certificates verified in LavinMQ's own TLS handshake are trusted,
      # not certificate details from a PROXY protocol header. A connection a
      # follower proxies to the leader is fine, as the leader terminates TLS.
      class External < Mechanism
        # Whether EXTERNAL can be used on this connection, so whether it's
        # advertised in Connection.Start.
        def self.available?(connection_info : ConnectionInfo) : Bool
          !Config.instance.external_auth_login_from.nil? && verified_certificate?(connection_info)
        end

        # The client presented a certificate that LavinMQ itself verified
        def self.verified_certificate?(connection_info) : Bool
          connection_info.tls_terminated? && connection_info.ssl_verify?
        end

        def credentials(response : String, connection_info : ConnectionInfo) : Tuple(String, String)
          login_from = Config.instance.external_auth_login_from
          raise Error.new("EXTERNAL authentication is not enabled") unless login_from
          unless External.verified_certificate?(connection_info)
            raise Error.new("EXTERNAL authentication requires a verified TLS client certificate")
          end
          case login_from
          when "common_name"
            ssl_cn = connection_info.ssl_cn || raise Error.new("Client certificate has no Common Name")
            {ssl_cn, ""}
          when "subject_alternative_name"
            from_subject_alternative_name(connection_info)
          else
            raise Error.new("Unsupported external_auth login_from '#{login_from}'")
          end
        end

        # Filters the SAN entries by the configured type (e.g. "DNS", "email",
        # "URI"), then picks the entry at the configured index within the
        # matches. An unset type matches all entries; an unset index defaults to
        # the first match.
        private def from_subject_alternative_name(connection_info) : Tuple(String, String)
          san_entries = connection_info.ssl_san_entries || raise Error.new("Client certificate has no Subject Alternative Name")
          san_type = Config.instance.external_auth_san_type
          san_index = Config.instance.external_auth_san_index || 0
          matching = san_entries.select do |san|
            san_type.nil? || san.type.compare(san_type, case_insensitive: true).zero?
          end
          ssl_san = (matching[san_index]? if san_index >= 0) ||
                    raise Error.new("Client certificate has no Subject Alternative Name " \
                                    "of type #{san_type || "any"} at index #{san_index}")
          {ssl_san.value, ""}
        end
      end
    end
  end
end
