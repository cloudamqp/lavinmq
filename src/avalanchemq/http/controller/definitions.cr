require "../controller"

module AvalancheMQ
  class DefinitionsController < Controller
    private def register_routes
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
  end
end
