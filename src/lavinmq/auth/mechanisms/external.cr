require "../../config"
require "./mechanism"

module LavinMQ
  module Auth
    module Mechanisms
      # SASL EXTERNAL: derives the identity from the client x509 certificate,
      # based on the configured `external_auth_login_from`.
      class External < Mechanism
        def credentials(response : String, connection_info : ConnectionInfo) : Tuple(String, String)
          case Config.instance.external_auth_login_from
          when "subject_alternative_name"
            from_subject_alternative_name(connection_info)
          when "common_name"
            ssl_cn = connection_info.ssl_cn || raise "EXTERNAL authentication method but no SSL Common Name present"
            {ssl_cn, ""}
          else
            raise "EXTERNAL is not configured on the server"
          end
        end

        # Filters the SAN entries by the configured type (e.g. "DNS", "email",
        # "URI"), then picks the entry at the configured index within the
        # matches. An unset type matches all entries; an unset index defaults to
        # the first match.
        private def from_subject_alternative_name(connection_info) : Tuple(String, String)
          san_entries = connection_info.ssl_san_entries || raise "EXTERNAL authentication method but no SAN found"
          san_type = Config.instance.external_auth_san_type
          san_index = Config.instance.external_auth_san_index || 0
          matching = san_entries.select do |san|
            san_type.nil? || san.type.compare(san_type, case_insensitive: true).zero?
          end
          ssl_san = matching[san_index]? || raise "EXTERNAL authentication method is missing SAN"
          {ssl_san.value, ""}
        end
      end
    end
  end
end
