require "../controller"

module AvalancheMQ
  class ParametersController < Controller
    private def register_routes
      get "/api/parameters" do |context, _params|
        user = user(context)
        refuse_unless_policymaker(context, user)
        vhosts(user)
          .flat_map { |v| v.parameters.values.map { |p| map_parameter(v.name, p) } }
          .to_json(context.response)
        context
      end

      get "/api/parameters/:component" do |context, params|
        user = user(context)
        refuse_unless_policymaker(context, user)
        component = URI.unescape(params["component"])
        vhosts(user)
          .flat_map { |v| v.parameters.values.map { |p| map_parameter(v.name, p) } }
          .select { |p| p["component"] == component }
          .to_json(context.response)
        context
      end

      get "/api/parameters/:component/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_policymaker(context, user(context), vhost)
          component = URI.unescape(params["component"])
          @amqp_server.vhosts[vhost].parameters.values
            .select { |p| p.component_name == component }
            .map { |p| map_parameter(vhost, p) }
            .to_json(context.response)
        end
      end

      get "/api/parameters/:component/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_policymaker(context, user(context), vhost)
          component = URI.unescape(params["component"])
          name = URI.unescape(params["name"])
          param = param(context, @amqp_server.vhosts[vhost].parameters, {component, name})
          map_parameter(vhost, param).to_json(context.response)
        end
      end

      put "/api/parameters/:component/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_policymaker(context, user(context), vhost)
          component = URI.unescape(params["component"])
          name = URI.unescape(params["name"])
          body = parse_body(context)
          value = body["value"]?
          unless value
            bad_request(context, "Field 'value' is required")
          end
          p = Parameter.new(component, name, value)
          @amqp_server.vhosts[vhost].add_parameter(p)
          context.response.status_code = 204
        end
      end

      delete "/api/parameters/:component/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_policymaker(context, user(context), vhost)
          component = URI.unescape(params["component"])
          name = URI.unescape(params["name"])
          param(context, @amqp_server.vhosts[vhost].parameters, {component, name})
          @amqp_server.vhosts[vhost].delete_parameter(component, name)
          context.response.status_code = 204
        end
      end

      get "/api/global-parameters" do |context, _params|
        refuse_unless_administrator(context, user(context))
        @amqp_server.parameters.values
          .map { |p| map_parameter(nil, p) }
          .to_json(context.response)
        context
      end

      get "/api/global-parameters/:name" do |context, params|
        refuse_unless_administrator(context, user(context))
        name = URI.unescape(params["name"])
        param = param(context, @amqp_server.parameters, Tuple.new(nil, name))
        map_parameter(nil, param).to_json(context.response)
        context
      end

      put "/api/global-parameters/:name" do |context, params|
        refuse_unless_administrator(context, user(context))
        name = URI.unescape(params["name"])
        body = parse_body(context)
        value = body["value"]?
        unless value
          bad_request(context, "Field 'value' is required")
        end
        p = Parameter.new(nil, name, value)
        @amqp_server.add_parameter(p)
        context.response.status_code = 204
        context
      end

      delete "/api/global-parameters/:name" do |context, params|
        refuse_unless_policymaker(context, user(context))
        name = URI.unescape(params["name"])
        param(context, @amqp_server.parameters, {nil, name})
        @amqp_server.delete_parameter(nil, name)
        context.response.status_code = 204
        context
      end

      get "/api/policies" do |context, _params|
        user = user(context)
        refuse_unless_policymaker(context, user)
        vhosts(user).flat_map { |v| v.policies.values }.to_json(context.response)
        context
      end

      get "/api/policies/:vhost" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_policymaker(context, user(context), vhost)
          @amqp_server.vhosts[vhost].policies.values.to_json(context.response)
        end
      end

      get "/api/policies/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_policymaker(context, user(context), vhost)
          name = URI.unescape(params["name"])
          p = policy(context, name, vhost)
          p.to_json(context.response)
        end
      end

      put "/api/policies/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_policymaker(context, user(context), vhost)
          name = URI.unescape(params["name"])
          body = parse_body(context)
          pattern = body["pattern"]?.try &.as_s?
          definition = body["definition"]?.try &.as_h
          priority = body["priority"]?.try &.as_i? || 0
          apply_to = body["apply-to"]?.try &.as_s? || "all"
          apply = Policy::Target.parse(apply_to)
          unless pattern && definition
            bad_request(context, "Fields 'pattern' and 'definition' are required")
          end
          @amqp_server.vhosts[vhost]
            .add_policy(name, Regex.new(pattern), apply, definition, priority.to_i8)
          context.response.status_code = 204
        end
      end

      delete "/api/policies/:vhost/:name" do |context, params|
        with_vhost(context, params) do |vhost|
          refuse_unless_policymaker(context, user(context), vhost)
          name = URI.unescape(params["name"])
          policy(context, name, vhost)
          @amqp_server.vhosts[vhost].delete_policy(name)
          context.response.status_code = 204
        end
      end
    end

    private def map_parameter(vhost, p)
      {
        "name"      => p.parameter_name,
        "component" => p.component_name,
        "vhost"     => vhost,
        "value"     => p.value,
      }.compact
    end

    private def param(context, parameters, id)
      param = parameters[id]?
      unless param
        not_found(context, "Parameter '#{id[1]}' does not exist")
      end
      param
    end

    private def policy(context, name, vhost)
      p = @amqp_server.vhosts[vhost].policies[name]?
      unless p
        not_found(context, "Policy '#{name}' on vhost '#{vhost}' does not exist")
      end
      p
    end
  end
end
