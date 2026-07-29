require "./vhost"

module LavinMQ
  abstract class DefinitionsImporter
    Log = LavinMQ::Log.for "definitions"

    def initialize(@amqp_server : LavinMQ::Server)
    end

    abstract def import(body, skip_existing)
    abstract def export(response)
    abstract def fetch_vhost?(json)
    abstract def vhosts

    # The queue/exchange/binding imports store definitions with fsync: false to
    # avoid an fsync per object; this flushes each vhost's definitions file once
    # at the end instead.
    private def fsync_definition_files
      vhosts.each_value(&.fsync_definitions)
    end

    private def export_vhost_parameters(json)
      json.array do
        vhosts.each_value do |vhost|
          # parameters
          vhost.parameters.each_value do |p|
            {
              name:      p.parameter_name,
              component: p.component_name,
              vhost:     vhost.name,
              value:     p.value,
            }.to_json(json)
          end
          # vhost-limits
          limits = Hash(String, Int32).new
          if mq = vhost.max_queues
            limits["max-queues"] = mq
          end
          if mc = vhost.max_connections
            limits["max-connections"] = mc
          end
          unless limits.empty?
            {
              component: "vhost-limits",
              vhost:     vhost.name,
              name:      "limits",
              value:     limits,
            }.to_json(json)
          end
          # operator policies
          vhost.operator_policies.each_value do |op|
            {
              component: "operator_policy",
              vhost:     vhost.name,
              name:      op.name,
              value:     {
                pattern:    op.pattern,
                definition: op.definition,
                priority:   op.priority,
                "apply-to": op.apply_to,
              },
            }.to_json(json)
          end
        end
      end
    end

    private def import_vhosts(body)
      if vhosts = body["vhosts"]?
        # Create with save: false so each vhost doesn't rewrite+fsync vhosts.json
        # (and users.json, via the permissions create adds); save both once at the end.
        vhosts.as_a.each do |v|
          name = v["name"].as_s
          @amqp_server.vhosts.create name, save: false
        end
        @amqp_server.vhosts.save!
        @amqp_server.users.save!
      end
    end

    private def import_queues(body)
      if queues = body["queues"]?
        queues.as_a.each do |q|
          if v = fetch_vhost?(q)
            name = q["name"].as_s
            durable = q["durable"].as_bool
            auto_delete = q["auto_delete"].as_bool
            arguments = AMQP::Table.new(q["arguments"].as_h)
            v.declare_queue(name, durable, auto_delete, arguments, fsync: false)
          end
        end
      end
    end

    private def import_exchanges(body)
      if exchanges = body["exchanges"]?
        exchanges.as_a.each do |e|
          if v = fetch_vhost?(e)
            name = e["name"].as_s
            type = e["type"].as_s
            durable = e["durable"].as_bool
            internal = e["internal"].as_bool
            auto_delete = e["auto_delete"].as_bool
            arguments = AMQP::Table.new(e["arguments"].as_h)
            v.declare_exchange(name, type, durable, auto_delete, internal, arguments, fsync: false)
          end
        end
      end
    end

    private def import_bindings(body)
      if bindings = body["bindings"]?
        bindings.as_a.each do |b|
          if v = fetch_vhost?(b)
            source = b["source"].as_s
            destination = b["destination"].as_s
            destination_type = b["destination_type"].as_s
            routing_key = b["routing_key"].as_s
            arguments = AMQP::Table.new(b["arguments"].as_h?)
            case destination_type
            when "queue"
              v.bind_queue(destination, source, routing_key, arguments, fsync: false)
            when "exchange"
              v.bind_exchange(destination, source, routing_key, arguments, fsync: false)
            end
          end
        end
      end
    end

    private def import_permissions(body, skip_existing = false)
      if permissions = body["permissions"]?
        permissions.as_a.each do |p|
          vhost = p["vhost"].as_s
          user = p["user"].as_s
          next if skip_existing && @amqp_server.users[user]?.try(&.permissions[vhost]?)
          configure = p["configure"].as_s
          read = p["read"].as_s
          write = p["write"].as_s
          unless @amqp_server.users[user]?
            Log.warn { "No user named #{user}, can't import permissions" }
            next
          end
          @amqp_server.users.add_permission(user, vhost,
            parse_regex(configure, "configure", user, vhost),
            parse_regex(read, "read", user, vhost),
            parse_regex(write, "write", user, vhost),
            save: false)
        end
        @amqp_server.users.save!
      end
    end

    private def parse_regex(pattern, field, user, vhost)
      Regex.new(pattern)
    rescue ex : ArgumentError
      raise ArgumentError.new(
        "Invalid regex in #{field} permission for user '#{user}' " \
        "in vhost '#{vhost}': #{ex.message}"
      )
    end

    private def import_users(body, skip_existing = false)
      if users = body["users"]?
        users.as_a.each do |u|
          name = u["name"].as_s
          next if skip_existing && @amqp_server.users[name]?
          pass_hash = parse_user_password_hash(u)
          hash_algo = parse_user_hash_algo(u)
          parsed_tags = parse_user_tags(u)
          @amqp_server.users.add(name, pass_hash, hash_algo, parsed_tags, save: false)
        end
        @amqp_server.users.save!
      end
    end

    private def parse_user_password_hash(u : JSON::Any) : String
      raise ArgumentError.new("Field 'password_hash' is required for each user") unless u["password_hash"]?
      raw = u["password_hash"]
      raise ArgumentError.new("Field 'password_hash' must be a string or null") unless raw.raw.nil? || raw.raw.is_a?(String)
      raw.as_s? || ""
    end

    private def parse_user_hash_algo(u : JSON::Any) : String?
      raw = u["hashing_algorithm"]?
      raise ArgumentError.new("Field 'hashing_algorithm' must be a string or null") if raw && !raw.raw.nil? && !raw.raw.is_a?(String)
      raw.try(&.as_s?)
    end

    private def parse_user_tags(u : JSON::Any) : Array(Tag)
      if tags = u["tags"]?.try &.as_s?
        tags.split(",").compact_map { |t| Tag.parse?(t.strip) }
      elsif tags = u["tags"]?.try &.as_a?
        tags.compact_map { |t| Tag.parse?(t.as_s) }
      else
        [] of Tag
      end
    end

    private def import_parameters(body, skip_existing = false)
      if parameters = body["parameters"]?
        # Parse and validate every entry before applying any, so a malformed
        # entry makes the whole import a clean no-op instead of partially
        # applying and leaving memory and disk inconsistent (issue #2073).
        limits = Array({VHost, Hash(String, JSON::Any)}).new
        operator_policies = Array({VHost, String, String, String, Hash(String, JSON::Any), Int8}).new
        params = Array({VHost, Parameter}).new
        parameters.as_a.each do |p|
          next unless v = fetch_vhost?(p)
          name = p["name"].as_s
          value = p["value"].as_h
          case component = p["component"].as_s
          when "vhost-limits"
            limits << {v, value}
          when "operator_policy"
            next if skip_existing && v.operator_policies[name]?
            operator_policies << {v, name, value["pattern"].as_s, value["apply-to"].as_s,
                                  value["definition"].as_h, value["priority"].as_i.to_i8}
          else
            next if skip_existing && v.parameters[{component, name}]?
            params << {v, Parameter.new(component, name, p["value"])}
          end
        end

        # add with save: false and save each touched store once at the end, so a
        # large import doesn't rewrite the whole parameters/operator_policies
        # JSON file per entry.
        touched_parameters = Set(VHost).new
        touched_operator_policies = Set(VHost).new
        limits.each { |v, value| import_vhost_limits(v, value, skip_existing) }
        operator_policies.each do |v, name, pattern, apply_to, definition, priority|
          v.add_operator_policy(name, pattern, apply_to, definition, priority, save: false, apply: false)
          touched_operator_policies << v
        end
        params.each do |v, param|
          v.add_parameter(param, save: false, apply: false)
          touched_parameters << v
        end
        touched_parameters.each(&.save_parameters!)
        touched_operator_policies.each(&.save_operator_policies!)
        apply_policies(touched_parameters | touched_operator_policies)
      end
    end

    private def import_vhost_limits(v, value, skip_existing)
      return if skip_existing && (v.max_connections || v.max_queues)
      if mc = value["max-connections"]?.try &.as_i?
        v.max_connections = mc
      end
      if mq = value["max-queues"]?.try &.as_i?
        v.max_queues = mq
      end
    end

    private def import_global_parameters(body, skip_existing = false)
      if parameters = body["global_parameters"]?
        # Parse all entries before applying any, so a malformed entry makes the
        # whole import a clean no-op (issue #2073).
        parsed = parameters.as_a.compact_map do |p|
          name = p["name"].as_s
          next if skip_existing && @amqp_server.parameters[{nil, name}]?
          Parameter.new(nil, name, p["value"])
        end
        return if parsed.empty?
        parsed.each { |param| @amqp_server.add_parameter(param, save: false) }
        @amqp_server.save_parameters!
      end
    end

    private def import_policies(body, skip_existing = false)
      if policies = body["policies"]?
        # Parse and validate every entry before applying any, so a malformed
        # entry makes the whole import a clean no-op instead of partially
        # applying and leaving memory and disk inconsistent (issue #2073).
        parsed = Array({VHost, String, String, String, Hash(String, JSON::Any), Int8}).new
        policies.as_a.each do |p|
          next unless v = fetch_vhost?(p)
          name = p["name"].as_s
          next if skip_existing && v.policies[name]?
          parsed << {v, name, p["pattern"].as_s, p["apply-to"].as_s,
                     p["definition"].as_h, p["priority"].as_i.to_i8}
        end
        touched = Set(VHost).new
        parsed.each do |v, name, pattern, apply_to, definition, priority|
          v.add_policy(name, pattern, apply_to, definition, priority, save: false, apply: false)
          touched << v
        end
        touched.each(&.save_policies!)
        apply_policies(touched)
      end
    end

    # Apply policies once per vhost after a bulk import, instead of per entry.
    private def apply_policies(vhosts : Set(VHost))
      vhosts.each do |v|
        spawn v.apply_policies, name: "ApplyPolicies (import) #{v.name}"
      end
    end

    private def export_policies(json)
      json.array do
        vhosts.each_value do |v|
          v.policies.each_value(&.to_json(json))
        end
      end
    end

    private def export_queues(json)
      json.array do
        vhosts.each_value do |v|
          v.each_queue do |q|
            next if q.exclusive?
            {
              "name":        q.name,
              "vhost":       q.vhost.name,
              "durable":     q.durable?,
              "auto_delete": q.auto_delete?,
              "arguments":   q.arguments,
            }.to_json(json)
          end
          v.each_session do |s|
            next unless s.durable?
            {
              "name":        s.name,
              "vhost":       s.vhost.name,
              "durable":     s.durable?,
              "auto_delete": s.auto_delete?,
              "arguments":   s.arguments,
            }.to_json(json)
          end
        end
      end
    end

    private def export_exchanges(json)
      json.array do
        vhosts.each_value do |v|
          v.exchanges.reject(&.internal?).each do |e|
            delayed = e.arguments["x-delayed-exchange"]?
            if delayed
              arguments = e.arguments.clone
              arguments["x-delayed-type"] = e.type
              arguments.delete("x-delayed-exchange")
            end
            {
              "name":        e.name,
              "vhost":       e.vhost.name,
              "type":        delayed ? "x-delayed-exchange" : e.type,
              "durable":     e.durable?,
              "auto_delete": e.auto_delete?,
              "internal":    e.internal?,
              "arguments":   delayed ? arguments : e.arguments,
            }.to_json(json)
          end
        end
      end
    end

    private def export_bindings(json)
      json.array do
        vhosts.each_value do |v|
          v.each_exchange do |e|
            e.bindings_details.each do |b|
              b.to_json(json)
            end
          end
        end
      end
    end

    private def export_permissions(json)
      json.array do
        @amqp_server.users.values.reject(&.hidden?).each do |u|
          u.permissions_details.each do |p|
            p.to_json(json)
          end
        end
      end
    end

    private def export_users(json)
      json.array do
        @amqp_server.users.values.reject(&.hidden?).each do |u|
          {
            "hashing_algorithm": u.user_details["hashing_algorithm"],
            "name":              u.name,
            "password_hash":     u.user_details["password_hash"],
            "tags":              u.tags,
          }.to_json(json)
        end
      end
    end
  end

  class VHostDefinitions < DefinitionsImporter
    def initialize(@amqp_server, @vhost : VHost)
      super(@amqp_server)
      @vhosts = {@vhost.name => @vhost}
    end

    getter vhosts : Hash(String, VHost)

    def import(body, skip_existing = false)
      import_queues(body)
      import_exchanges(body)
      import_bindings(body)
      fsync_definition_files
      import_policies(body, skip_existing)
      import_parameters(body, skip_existing)
    end

    def export(response)
      JSON.build(response) do |json|
        json.object do
          json.field("lavinmq_version", LavinMQ::VERSION)
          json.field("queues") { export_queues(json) }
          json.field("exchanges") { export_exchanges(json) }
          json.field("bindings") { export_bindings(json) }
          json.field("policies") { export_policies(json) }
          json.field("parameters") { export_vhost_parameters(json) }
        end
      end
    end

    def fetch_vhost?(json) # ignore vhost property, always use the specified one
      @vhost
    end
  end

  class GlobalDefinitions < DefinitionsImporter
    def self.import_from_file(path : String, amqp_server : Server)
      Log.info { "Importing definitions from #{path}" }
      body = JSON.parse(File.read(path))
      new(amqp_server).import(body, skip_existing: true)
      Log.info { "Definitions imported from #{path}" }
    end

    def import(body, skip_existing = false)
      import_users(body, skip_existing)
      import_permissions(body, skip_existing)
      import_vhosts(body)
      import_queues(body)
      import_exchanges(body)
      import_bindings(body)
      fsync_definition_files
      import_policies(body, skip_existing)
      import_parameters(body, skip_existing)
      import_global_parameters(body, skip_existing)
    end

    def export(response)
      JSON.build(response) do |json|
        json.object do
          json.field("lavinmq_version", LavinMQ::VERSION)
          json.field("users") { export_users(json) }
          json.field("vhosts", @amqp_server.vhosts)
          json.field("permissions") { export_permissions(json) }
          json.field("queues") { export_queues(json) }
          json.field("exchanges") { export_exchanges(json) }
          json.field("bindings") { export_bindings(json) }
          json.field("policies") { export_policies(json) }
          json.field("parameters") { export_vhost_parameters(json) }
          json.field("global_parameters") { @amqp_server.parameters.to_json(json) }
        end
      end
    end

    def vhosts
      @amqp_server.vhosts
    end

    def fetch_vhost?(json)
      if name = json["vhost"]?.try(&.as_s)
        if vhost = vhosts[name]?
          vhost
        else
          Log.warn { "No vhost named #{name}, can't import #{name}" }
        end
      elsif vhost = vhosts.first_value?
        vhost # if vhost property is missing, use first/default vhost
      else
        Log.warn { "No vhost defined, can't import entry without vhost" }
      end
    end
  end
end
