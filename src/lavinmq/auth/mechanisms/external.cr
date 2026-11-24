require "../../config"
require "./mechanism"

module LavinMQ
  module Auth
    module Mechanisms
      class External < Mechanism
        def self.available?(connection_info : ConnectionInfo) : Bool
          !Config.instance.external_auth_login_from.nil? && verified_certificate?(connection_info)
        end

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
