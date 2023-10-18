require "uri"
require "../controller"
require "../../vhost"

module LavinMQ
  module HTTP
    class DefinitionsController < Controller
      private def register_routes
        get "/api/definitions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          GlobalDefinitions.new(@amqp_server).export(context.response)
          context
        end

        post "/api/definitions" do |context, _params|
          refuse_unless_administrator(context, user(context))
          body = parse_body(context)
          GlobalDefinitions.new(@amqp_server).import(body)
          context
        end

        post "/api/definitions/upload" do |context, _params|
          refuse_unless_administrator(context, user(context))
          if context.request.headers["Content-Type"] == "application/json"
            body = parse_body(context)
            GlobalDefinitions.new(@amqp_server).import(body)
          else
            ::HTTP::FormData.parse(context.request) do |part|
              if part.name == "file"
                body = JSON.parse(part.body)
                GlobalDefinitions.new(@amqp_server).import(body)
              end
            end
          end
          redirect_back(context) if context.request.headers["Referer"]?
          context
        end

        get "/api/definitions/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_management(context, user(context), vhost)
            refuse_unless_vhost_access(context, user(context), vhost)
            unescaped_name = URI.decode_www_form(vhost)
            VHostDefinitions.new(@amqp_server, @amqp_server.vhosts[unescaped_name]).export(context.response)
          end
        end

        post "/api/definitions/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            refuse_unless_vhost_access(context, user(context), vhost)
            body = parse_body(context)
            unescaped_name = URI.decode_www_form(vhost)
            VHostDefinitions.new(@amqp_server, @amqp_server.vhosts[unescaped_name]).import(body)
          end
        end

        post "/api/definitions/:vhost/upload" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            refuse_unless_vhost_access(context, user(context), vhost)
            ::HTTP::FormData.parse(context.request) do |part|
              if part.name == "file"
                body = JSON.parse(part.body)
                unescaped_name = URI.decode_www_form(vhost)
                VHostDefinitions.new(@amqp_server, @amqp_server.vhosts[unescaped_name]).import(body)
              end
            end
          end
        end
      end

      abstract class Definitions
        Log = ::Log.for(self)

        def initialize(@amqp_server : LavinMQ::Server)
        end

        abstract def import(body)
        abstract def export(response)
        abstract def fetch_vhost?(json)
        abstract def vhosts

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
            vhosts.as_a.each do |v|
              name = v["name"].as_s
              @amqp_server.vhosts.create name
            end
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
                v.declare_queue(name, durable, auto_delete, arguments)
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
                v.declare_exchange(name, type, durable, auto_delete, internal, arguments)
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
                  v.bind_queue(destination, source, routing_key, arguments)
                when "exchange"
                  v.bind_exchange(destination, source, routing_key, arguments)
                end
              end
            end
          end
        end

        private def import_permissions(body)
          if permissions = body["permissions"]?
            permissions.as_a.each do |p|
              vhost = p["vhost"].as_s
              user = p["user"].as_s
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
        end

        private def import_users(body)
          if users = body["users"]?
            users.as_a.each do |u|
              name = u["name"].as_s
              pass_hash = u["password_hash"].as_s
              hash_algo = u["hashing_algorithm"]?.try(&.as_s)
              tags = u["tags"]?.to_s.gsub(/[\[\]"\s]/, "").split(",").compact_map { |t| Tag.parse?(t) }
              @amqp_server.users.add(name, pass_hash, hash_algo, tags, save: false)
            end
            @amqp_server.users.save!
          end
        end

        private def import_parameters(body)
          if parameters = body["parameters"]?
            parameters.as_a.each do |p|
              if v = fetch_vhost?(p)
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
          end
        end

        private def import_global_parameters(body)
          if parameters = body["global_parameters"]?
            parameters.as_a.each do |p|
              param = Parameter.new(nil, p["name"].as_s, p["value"])
              @amqp_server.add_parameter(param)
            end
          end
        end

        private def import_policies(body)
          if policies = body["policies"]?
            policies.as_a.each do |p|
              if v = fetch_vhost?(p)
                v.add_policy(
                  p["name"].as_s,
                  p["pattern"].as_s,
                  p["apply-to"].as_s,
                  p["definition"].as_h,
                  p["priority"].as_i.to_i8)
              end
            end
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
              v.queues.each_value do |q|
                next if q.exclusive?
                {
                  "name":        q.name,
                  "vhost":       q.vhost.name,
                  "durable":     q.durable?,
                  "auto_delete": q.auto_delete?,
                  "arguments":   q.arguments,
                }.to_json(json)
              end
            end
          end
        end

        private def export_exchanges(json)
          json.array do
            vhosts.each_value do |v|
              v.exchanges.each_value.reject(&.internal?).each do |e|
                {
                  "name":        e.name,
                  "vhost":       e.vhost.name,
                  "type":        e.type,
                  "durable":     e.durable?,
                  "auto_delete": e.auto_delete?,
                  "internal":    e.internal?,
                  "arguments":   e.arguments,
                }.to_json(json)
              end
            end
          end
        end

        private def export_bindings(json)
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

      class VHostDefinitions < Definitions
        def initialize(@amqp_server, @vhost : VHost)
          super(@amqp_server)
          @vhosts = {@vhost.name => @vhost}
        end

        getter vhosts : Hash(String, VHost)

        def import(body)
          import_queues(body)
          import_exchanges(body)
          import_bindings(body)
          import_policies(body)
          import_parameters(body)
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

      class GlobalDefinitions < Definitions
        def import(body)
          import_users(body)
          import_vhosts(body)
          import_permissions(body)
          import_queues(body)
          import_exchanges(body)
          import_bindings(body)
          import_policies(body)
          import_parameters(body)
          import_global_parameters(body)
        end

        def export(response)
          JSON.build(response) do |json|
            json.object do
              json.field("lavinmq_version", LavinMQ::VERSION)
              json.field("users", @amqp_server.users.values.reject(&.hidden?))
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
          else # if vhost property is missing, use first/default vhost
            vhosts.first_value
          end
        end
      end
    end
  end
end
