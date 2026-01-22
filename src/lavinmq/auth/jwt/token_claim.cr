module LavinMQ
  module Auth
    module JWT
      record TokenClaim,
        username : String,
        tags : Array(Tag),
        permissions : Hash(String, User::Permissions),
        expires_at : Time

      class TokenParser
        def initialize(@config : Config)
        end

        def parse(token : JWT::Token) : TokenClaim
          payload = token.payload
          username = extract_username(payload)
          tags, permissions = parse_roles(payload)
          expires_at = payload["exp"]?.try(&.as_i64?)
          raise JWT::DecodeError.new("No expiration time found in JWT token") unless expires_at
          TokenClaim.new(username, tags, permissions, Time.unix(expires_at))
        end

        private def extract_username(payload) : String
          claims = @config.oauth_preferred_username_claims
          claims.each do |claim|
            if username = payload[claim]?.try(&.as_s?)
              return username
            end
          end
          raise JWT::DecodeError.new("No username found in JWT claims (tried: #{claims.join(", ")})")
        end

        # Extracts roles/scopes from JWT payload and converts to LavinMQ tags and permissions.
        private def parse_roles(payload)
          scopes = [] of String

          if server_id = @config.oauth_resource_server_id
            if arr = payload.dig?("resource_access", server_id, "roles").try(&.as_a?)
              scopes.concat(arr.map(&.as_s))
            end
          end

          if scope_str = payload["scope"]?.try(&.as_s?)
            scopes.concat(scope_str.split)
          end

          if scopes_key = @config.oauth_additional_scopes_key
            if claim = payload[scopes_key]?
              scopes.concat(extract_scopes_from_claim(claim))
            end
          end

          tags = Set(Tag).new
          permissions = Hash(String, User::Permissions).new
          filter_scopes(scopes).each { |scope| parse_role(scope, tags, permissions) }
          {tags.to_a, permissions}
        end

        private def extract_scopes_from_claim(claim) : Array(String)
          case claim
          when .as_h?
            claim.as_h.flat_map do |key, value|
              if !@config.oauth_resource_server_id || key == @config.oauth_resource_server_id
                extract_scopes_from_claim(value)
              end
            end.compact
          when .as_a?
            claim.as_a.flat_map do |item|
              extract_scopes_from_claim(item)
            end
          when .as_s?
            claim.as_s.split
          else
            Array(String).new(0)
          end
        end

        private def filter_scopes(scopes : Array(String)) : Array(String)
          prefix = @config.oauth_scope_prefix
          if !prefix && @config.oauth_resource_server_id
            prefix = "#{@config.oauth_resource_server_id}."
          end

          return scopes unless prefix

          scopes.compact_map do |scope|
            scope[prefix.size..] if scope.starts_with?(prefix)
          end
        end

        private def parse_role(role, tags, permissions)
          if role.starts_with?("tag:")
            parse_tag_role(role, tags)
          else
            parse_permission_role(role, permissions)
          end
        end

        private def parse_tag_role(role, tags)
          tag_name = role[4..]
          if tag = Tag.parse?(tag_name)
            tags << tag
          end
        end

        private def parse_permission_role(role, permissions)
          parts = role.split(/[.:\/]/)
          if parts.size != 3
            Log.warn { "Skipping scope '#{role}': Expected format 'permission:vhost:pattern'" }
            return
          end
          perm_type, vhost, pattern = parts[0], parts[1], parts[2]
          return if !perm_type.in?("configure", "read", "write")

          pattern = ".*" if pattern == "*"

          permissions[vhost] ||= {
            config: Regex.new("^$"),
            read:   Regex.new("^$"),
            write:  Regex.new("^$"),
          }

          begin
            regex = Regex.new(pattern)
          rescue ex : ArgumentError
            Log.warn { "Skipping scope '#{role}' due to invalid regex pattern: #{ex.message}" }
            return
          end

          permissions[vhost] = case perm_type
                               when "configure" then permissions[vhost].merge({config: regex})
                               when "config"    then permissions[vhost].merge({config: regex})
                               when "read"      then permissions[vhost].merge({read: regex})
                               when "write"     then permissions[vhost].merge({write: regex})
                               else                  permissions[vhost]
                               end
        end
      end
    end
  end
end
