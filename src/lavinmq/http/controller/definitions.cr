require "uri"
require "../controller"

module LavinMQ
  module HTTP
    class DefinitionsController < Controller
      private def register_routes
        get "/api/definitions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          export_definitions(context.response)
          context
        end

        post "/api/definitions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          body = parse_body(context)
          import_definitions(body)
          context
        end

        post "/api/definitions/upload" do |context, _params|
          refuse_unless_administrator(context, user(context))
          ::HTTP::FormData.parse(context.request) do |part|
            if part.name == "file"
              body = JSON.parse(part.body)
              import_definitions(body)
            end
          end
          redirect_back(context) if context.request.headers["Referer"]?
          context
        end

        get "/api/definitions/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            refuse_unless_vhost_access(context, user(context), vhost)
            export_vhost_definitions(vhost, context.response)
          end
        end

        post "/api/definitions/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            refuse_unless_vhost_access(context, user(context), vhost)
            body = parse_body(context)
            import_vhost_definitions(vhost, body)
          end
        end

        post "/api/definitions/:vhost/upload" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            refuse_unless_vhost_access(context, user(context), vhost)
            ::HTTP::FormData.parse(context.request) do |part|
              if part.name == "file"
                body = JSON.parse(part.body)
                import_vhost_definitions(vhost, body)
              end
            end
          end
        end
      end

      private def import_vhost_definitions(name, body)
        unescaped_name = URI.decode_www_form(name)
        vhosts = {unescaped_name => @amqp_server.vhosts[unescaped_name]}
        import_queues(body, vhosts)
        import_exchanges(body, vhosts)
        import_bindings(body, vhosts)
        import_policies(body, vhosts)
        import_parameters(body, vhosts)
      end

      private def import_definitions(body)
        import_users(body)
        import_vhosts(body)
        import_queues(body, @amqp_server.vhosts)
        import_exchanges(body, @amqp_server.vhosts)
        import_bindings(body, @amqp_server.vhosts)
        import_permissions(body)
        import_policies(body, @amqp_server.vhosts)
        import_parameters(body, @amqp_server.vhosts)
        import_global_parameters(body)
      end

      private def export_vhost_definitions(name, response)
        unescaped_name = URI.decode_www_form(name)
        vhosts = {unescaped_name => @amqp_server.vhosts[unescaped_name]}
        JSON.build(response) do |json|
          json.object do
            json.field("lavinmq_version", LavinMQ::VERSION)
            json.field("queues") { export_queues(json, vhosts) }
            json.field("exchanges") { export_exchanges(json, vhosts) }
            json.field("bindings") { export_bindings(json, vhosts) }
            json.field("policies") { export_policies(json, vhosts) }
          end
        end
      end

      private def export_definitions(response)
        JSON.build(response) do |json|
          json.object do
            json.field("lavinmq_version", LavinMQ::VERSION)
            json.field("users", @amqp_server.users.values.reject(&.hidden?))
            json.field("vhosts", @amqp_server.vhosts)
            json.field("queues") { export_queues(json, @amqp_server.vhosts) }
            json.field("exchanges") { export_exchanges(json, @amqp_server.vhosts) }
            json.field("bindings") { export_bindings(json, @amqp_server.vhosts) }
            json.field("permissions") { export_permissions(json) }
            json.field("policies") { export_policies(json, @amqp_server.vhosts) }
            json.field("parameters") { export_vhost_parameters(json, @amqp_server.vhosts) }
            json.field("global_parameters") { @amqp_server.parameters.to_json(json) }
          end
        end
      end

      private def export_vhost_parameters(json, vhosts)
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

      private def fetch_vhost?(vhosts, name)
        vhost = vhosts[name]?
        @log.warn { "No vhost named #{name}, can't import #{name}" } unless vhost
        vhost
      end

      private def import_vhosts(body)
        return unless vhosts = body["vhosts"]?
        vhosts.as_a.each do |v|
          name = v["name"].as_s
          @amqp_server.vhosts.create name
        end
      end

      private def import_queues(body, vhosts)
        return unless queues = body["queues"]?
        queues.as_a.each do |q|
          name = q["name"].as_s
          vhost = q["vhost"].as_s
          durable = q["durable"].as_bool
          auto_delete = q["auto_delete"].as_bool
          arguments = AMQP::Table.new(q["arguments"].as_h)
          next unless v = fetch_vhost?(vhosts, vhost)
          v.declare_queue(name, durable, auto_delete, arguments)
        end
      end

      private def import_exchanges(body, vhosts)
        return unless exchanges = body["exchanges"]?
        exchanges.as_a.each do |e|
          name = e["name"].as_s
          vhost = e["vhost"].as_s
          type = e["type"].as_s
          durable = e["durable"].as_bool
          internal = e["internal"].as_bool
          auto_delete = e["auto_delete"].as_bool
          arguments = AMQP::Table.new(e["arguments"].as_h)
          next unless v = fetch_vhost?(vhosts, vhost)
          v.declare_exchange(name, type, durable, auto_delete, internal, arguments)
        end
      end

      private def import_bindings(body, vhosts)
        return unless bindings = body["bindings"]?
        bindings.as_a.each do |b|
          source = b["source"].as_s
          vhost = b["vhost"].as_s
          destination = b["destination"].as_s
          destination_type = b["destination_type"].as_s
          routing_key = b["routing_key"].as_s
          arguments = AMQP::Table.new(b["arguments"].as_h?)
          next unless v = fetch_vhost?(vhosts, vhost)
          case destination_type
          when "queue"
            v.bind_queue(destination, source, routing_key, arguments)
          when "exchange"
            v.bind_exchange(destination, source, routing_key, arguments)
          else nil
          end
        end
      end

      private def import_permissions(body)
        return unless permissions = body["permissions"]?
        permissions.as_a.each do |p|
          user = p["user"].as_s
          vhost = p["vhost"].as_s
          configure = p["configure"].as_s
          read = p["read"].as_s
          write = p["write"].as_s
          @amqp_server.users[user].permissions[vhost] = {
            config: Regex.new(configure),
            read:   Regex.new(read),
            write:  Regex.new(write),
          }
        end
        @amqp_server.users.save!
      end

      private def import_users(body)
        return unless users = body["users"]?
        users.as_a.each do |u|
          name = u["name"].as_s
          pass_hash = u["password_hash"].as_s
          hash_algo = u["hashing_algorithm"]?.try(&.as_s)
          tags = u["tags"]?.try(&.as_s).to_s.split(",").compact_map { |t| Tag.parse?(t) }
          @amqp_server.users.add(name, pass_hash, hash_algo, tags, save: false)
        end
        @amqp_server.users.save!
      end

      private def import_parameters(body, vhosts)
        return unless parameters = body["parameters"]?
        parameters.as_a.each do |p|
          vhost = p["vhost"].as_s
          next unless v = fetch_vhost?(vhosts, vhost)
          name = p["name"].as_s
          value = p["value"].as_h
          component = p["component"].as_s
          case component
          when "vhost-limits"
            if mc = value["max-connections"]?.try &.as_i?
              v.max_connections = mc
            end
            if mq = value["max-queues"]?.try &.as_i?
              v.max_queues = mq
            end
          when "operator_policy"
            v.add_operator_policy(name,
              value["pattern"].as_s,
              value["apply-to"].as_s,
              value["definition"].as_h,
              value["priority"].as_i.to_i8)
          else
            v.add_parameter(Parameter.new(component, name, p["value"]))
          end
        end
      end

      private def import_global_parameters(body)
        return unless parameters = body["global_parameters"]?
        parameters.as_a.each do |p|
          param = Parameter.new(nil, p["name"].as_s, p["value"])
          @amqp_server.add_parameter(param)
        end
      end

      private def import_policies(body, vhosts)
        return unless policies = body["policies"]?
        policies.as_a.each do |p|
          name = p["name"].as_s
          vhost = p["vhost"].as_s
          if v = fetch_vhost?(vhosts, vhost)
            v.add_policy(name,
              p["pattern"].as_s,
              p["apply-to"].as_s,
              p["definition"].as_h,
              p["priority"].as_i.to_i8)
          end
        end
      end

      private def export_policies(json, vhosts)
        json.array do
          vhosts.each_value do |v|
            v.policies.each_value(&.to_json(json))
          end
        end
      end

      private def export_queues(json, vhosts)
        json.array do
          vhosts.each_value do |v|
            v.queues.each_value do |q|
              {
                "name":        q.name,
                "vhost":       q.vhost.name,
                "durable":     q.durable,
                "auto_delete": q.auto_delete,
                "arguments":   q.arguments,
              }.to_json(json)
            end
          end
        end
      end

      private def export_exchanges(json, vhosts)
        json.array do
          vhosts.each_value do |v|
            v.exchanges.each_value.reject(&.internal).each do |e|
              {
                "name":        e.name,
                "vhost":       e.vhost.name,
                "type":        e.type,
                "durable":     e.durable,
                "auto_delete": e.auto_delete,
                "internal":    e.internal,
                "arguments":   e.arguments,
              }.to_json(json)
            end
          end
        end
      end

      private def export_bindings(json, vhosts)
        json.array do
          vhosts.each_value do |v|
            v.exchanges.each_value do |e|
              e.bindings_details.each do |b|
                b.to_json(json)
              end
            end
          end
        end
      end

      private def export_permissions(json)
        json.array do
          @amqp_server.users.each_value.reject(&.hidden?).each do |u|
            u.permissions_details.each do |p|
              p.to_json(json)
            end
          end
        end
      end
    end
  end
end
