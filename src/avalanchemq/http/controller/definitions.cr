require "../controller"

module AvalancheMQ
  class DefinitionsController < Controller
    private def register_routes
      get "/api/definitions" do |context, params|
        export_definitions(context.response)
        context
      end

      post "/api/definitions" do |context, _params|
        body = parse_body(context)
        import_definitions(body)
        context
      end
    end

    private def import_definitions(body)
      import_users(body)
      import_vhosts(body)
      import_queues(body)
      import_exchanges(body)
      import_bindings(body)
      import_permissions(body)
      import_policies(body)
      import_parameters(body)
    end

    private def export_definitions(response)
      {
        "avalanchemq_version": AvalancheMQ::VERSION,
        "users": export_users,
        "vhosts": @amqp_server.vhosts.map { |v| { name: v.name }},
        "queues": export_queues,
        "exchanges": export_exchanges,
        "bindings": export_bindings,
        "permissions": export_permissions,
        "policies": @amqp_server.vhosts.flat_map(&.policies).reject(&.empty?),
        "parameters": @amqp_server.parameters
      }.to_json(response)
    end

    private def import_vhosts(body)
      return unless vhosts = body["vhosts"]? || nil
      vhosts.each do |v|
        name = v["name"].as_s
        @amqp_server.vhosts.create name
      end
    end

    private def import_queues(body)
      return unless queues = body["queues"]? || nil
      queues.each do |q|
        name = q["name"].as_s
        vhost = q["vhost"].as_s
        durable = q["durable"].as_bool
        auto_delete = q["auto_delete"].as_bool
        json_args = q["arguments"].as_h
        arguments = Hash(String, AMQP::Field).new(json_args.size)
        json_args.each do |k, v|
          arguments[k] = v.as AMQP::Field
        end
        @amqp_server.vhosts[vhost].declare_queue(name, durable, auto_delete,
                                                  arguments)
      end
    end

    private def import_exchanges(body)
      return unless exchanges = body["exchanges"]? || nil
      exchanges.each do |e|
        name = e["name"].as_s
        vhost = e["vhost"].as_s
        type = e["type"].as_s
        durable = e["durable"].as_bool
        internal = e["internal"].as_bool
        auto_delete = e["auto_delete"].as_bool
        json_args = e["arguments"].as_h
        arguments = Hash(String, AMQP::Field).new(json_args.size)
        json_args.each do |k, v|
          arguments[k] = v.as AMQP::Field
        end
        @amqp_server.vhosts[vhost].declare_exchange(name, type, durable,
                                                    auto_delete, internal,
                                                    arguments)
      end
    end

    private def import_bindings(body)
      return unless bindings = body["bindings"]? || nil
      bindings.each do |b|
        source = b["source"].as_s
        vhost = b["vhost"].as_s
        destination = b["destination"].as_s
        destination_type = b["destination_type"].as_s
        routing_key = b["routing_key"].as_s
        json_args = b["arguments"].as_h
        arguments = Hash(String, AMQP::Field).new(json_args.size)
        json_args.each do |k, v|
          arguments[k] = v.as AMQP::Field
        end
        case destination_type
        when "queue"
          @amqp_server.vhosts[vhost].bind_queue(destination, source,
                                                routing_key, arguments)
        when "exchange"
          @amqp_server.vhosts[vhost].bind_exchange(destination, source,
                                                    routing_key, arguments)
        end
      end
    end

    private def import_permissions(body)
      return unless permissions = body["permissions"]? || nil
      permissions.each do |p|
        user = p["user"].as_s
        vhost = p["vhost"].as_s
        configure = p["configure"].as_s
        read = p["read"].as_s
        write = p["write"].as_s
        @amqp_server.users[user].permissions[vhost] = {
          config: Regex.new(configure),
          read: Regex.new(read),
          write: Regex.new(write)
        }
      end
    end

    private def import_users(body)
      return unless users = body["users"]? || nil
      users.each do |u|
        name = u["name"].as_s
        pass_hash = u["password_hash"].as_s
        hash_algo =
          case u["hashing_algorithm"]?.try(&.as_s) || nil
          when /sha256$/ then "SHA256"
          else "MD5"
          end
        @amqp_server.users.add(name, pass_hash, hash_algo, save: false)
      end
      @amqp_server.users.save!
    end

    private def import_parameters(body)
      return unless parameters = body["parameters"]? || nil
      parameters.each do |p|
        p = Parameter.new(p["component"].as_s, p["name"].as_s, p["value"])
        @amqp_server.add_parameter(p)
      end
    end

    private def import_policies(body)
      return unless policies = body["policies"]? || nil
      policies.each do |p|
        name = p["name"].as_s
        p = Policy.new(name, p["vhost"].as_s, Regex.new(p["pattern"].as_s),
                       Policy::Target.parse(p["apply-to"].as_s), p["definition"],
                       p["priority"].as_i.to_i8)
        vhost = @amqp_server.vhosts[p.vhost]? || nil
        unless vhost
          @log.warn "No vhost named #{p.vhost}, can't import #{name}"
          next
        end
        vhost.add_policy(p)
      end
    end

    private def export_users
      @amqp_server.users.map do |u|
        {
          "name": u.name,
          "hashing_algorithm": u.hash_algorithm,
          "password_hash": u.password.to_s
        }
      end
    end

    private def export_queues
      @amqp_server.vhosts.flat_map do |v|
        v.queues.values.map do |q|
          {
            "name": q.name,
            "vhost": q.vhost.name,
            "durable": q.durable,
            "auto_delete": q.auto_delete,
            "arguments": q.arguments
          }
        end
      end
    end

    private def export_exchanges
      @amqp_server.vhosts.flat_map do |v|
        v.exchanges.values.reject(&.internal).map do |e|
          {
            "name": e.name,
            "vhost": e.vhost.name,
            "type": e.type,
            "durable": e.durable,
            "auto_delete": e.auto_delete,
            "internal": e.internal,
            "arguments": e.arguments
          }
        end
      end
    end

    private def export_bindings
      bindings = Array(Hash(String, AMQP::Field)).new
      @amqp_server.vhosts.each do |v|
        v.exchanges.values.each do |e|
          e.bindings.each do |key, resources|
            resources.each do |resource|
              binding = {
                "source" => e.name,
                "vhost" => e.vhost.name,
                "destination" => resource.name,
                "destination_type" => resource.class.name.downcase,
                "routing_key" => key[0],
                "arguments" => key[1]
              } of String => AMQP::Field
              bindings << binding
            end
          end
        end
      end
      bindings
    end

    private def export_permissions
      @amqp_server.users.flat_map do |u|
        u.permissions.map do |vhost, permissions|
          {
            "user": u.name,
            "vhost": vhost,
            "configure": permissions[:config],
            "read": permissions[:read],
            "write": permissions[:write]
          }
        end
      end
    end
  end
end
