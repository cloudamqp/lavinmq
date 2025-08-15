require "../controller"
require "../../sortable_json"

module LavinMQ
  module HTTP
    struct ParameterView
      include SortableJSON

      def initialize(@p : Parameter, @vhost : String?)
      end

      def details_tuple
        {
          name:      @p.parameter_name,
          value:     @p.value,
          component: @p.component_name,
          vhost:     @vhost,
        }
      end

      def search_match?(value : String) : Bool
        @p.parameter_name.includes? value
      end

      def search_match?(value : Regex) : Bool
        value === @p.parameter_name
      end
    end

    class ParametersController < Controller
      private def register_routes # ameba:disable Metrics/CyclomaticComplexity
        get "/api/parameters" do |context, _params|
          user = user(context)
          refuse_unless_policymaker(context, user)
          itr = vhosts(user).flat_map do |v|
            v.parameters.each_value.map { |p| map_parameter(v.name, p) }
          end
          page(context, itr)
        end

        get "/api/parameters/:component" do |context, params|
          user = user(context)
          refuse_unless_policymaker(context, user)
          component = URI.decode_www_form(params["component"])
          itr = vhosts(user).flat_map do |v|
            v.parameters.each_value
              .select { |p| p.component_name == component }
              .map { |p| map_parameter(v.name, p) }
          end
          page(context, itr)
        end

        get "/api/parameters/:component/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            component = URI.decode_www_form(params["component"])
            itr = @amqp_server.vhosts[vhost].parameters.each_value
              .select { |p| p.component_name == component }
              .map { |p| map_parameter(vhost, p) }
            page(context, itr)
          end
        end

        get "/api/parameters/:component/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            component = URI.decode_www_form(params["component"])
            name = URI.decode_www_form(params["name"])
            param = param(context, @amqp_server.vhosts[vhost].parameters, {component, name})
            map_parameter(vhost, param).to_json(context.response)
          end
        end

        put "/api/parameters/:component/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            component = URI.decode_www_form(params["component"])
            name = URI.decode_www_form(params["name"])
            body = parse_body(context)
            value = body["value"]?
            unless value
              bad_request(context, "Field 'value' is required")
            end
            p = Parameter.new(component, name, value)
            is_update = @amqp_server.vhosts[vhost].parameters[{component, name}]?
            @amqp_server.vhosts[vhost].add_parameter(p)
            context.response.status_code = is_update ? 204 : 201
          end
        end

        delete "/api/parameters/:component/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            component = URI.decode_www_form(params["component"])
            name = URI.decode_www_form(params["name"])
            param(context, @amqp_server.vhosts[vhost].parameters, {component, name})
            @amqp_server.vhosts[vhost].delete_parameter(component, name)
            context.response.status_code = 204
          end
        end

        get "/api/global-parameters" do |context, _params|
          refuse_unless_administrator(context, user(context))
          page(context, @amqp_server.parameters.each_value.map { |p| map_parameter(nil, p) })
        end

        get "/api/global-parameters/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          name = URI.decode_www_form(params["name"])
          param = param(context, @amqp_server.parameters, {nil, name})
          map_parameter(nil, param).to_json(context.response)
          context
        end

        put "/api/global-parameters/:name" do |context, params|
          refuse_unless_administrator(context, user(context))
          name = URI.decode_www_form(params["name"])
          body = parse_body(context)
          value = body["value"]?
          unless value
            bad_request(context, "Field 'value' is required")
          end
          p = Parameter.new(nil, name, value)
          is_update = @amqp_server.parameters[{nil, name}]?
          @amqp_server.add_parameter(p)
          context.response.status_code = is_update ? 204 : 201
          context
        end

        delete "/api/global-parameters/:name" do |context, params|
          refuse_unless_policymaker(context, user(context))
          name = URI.decode_www_form(params["name"])
          param(context, @amqp_server.parameters, {nil, name})
          @amqp_server.delete_parameter(nil, name)
          context.response.status_code = 204
          context
        end

        get "/api/policies" do |context, _params|
          user = user(context)
          refuse_unless_policymaker(context, user)
          page(context, vhosts(user).flat_map(&.policies.values))
        end

        get "/api/policies/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            page(context, @amqp_server.vhosts[vhost].policies.each_value)
          end
        end

        get "/api/policies/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            name = URI.decode_www_form(params["name"])
            policy(context, name, vhost).to_json(context.response)
          end
        end

        put "/api/policies/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            name = URI.decode_www_form(params["name"])
            body = parse_body(context)
            pattern = body["pattern"]?.try &.as_s?
            definition = body["definition"]?.try &.as_h
            priority = body["priority"]?.try &.as_i? || 0
            apply_to = body["apply-to"]?.try &.as_s? || "all"
            unless pattern && definition
              bad_request(context, "Fields 'pattern' and 'definition' are required")
            end
            definition.keys.all? do |k|
              case k
              when "max-length", "max-length-bytes", "message-ttl", "expires", "delivery-limit"
                bad_request(context, "Policy definition '#{k}' should be of type Int") unless definition[k].as_i64?
              else
                bad_request(context, "Policy definition '#{k}' should be of type String") unless definition[k].as_s?
              end
            end
            is_update = @amqp_server.vhosts[vhost].policies[name]?
            @amqp_server.vhosts[vhost]
              .add_policy(name, pattern, apply_to, definition, priority.to_i8)
            context.response.status_code = is_update ? 204 : 201
          end
        end

        delete "/api/policies/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            name = URI.decode_www_form(params["name"])
            policy(context, name, vhost)
            @amqp_server.vhosts[vhost].delete_policy(name)
            context.response.status_code = 204
          end
        end

        get "/api/operator-policies" do |context, _params|
          user = user(context)
          refuse_unless_policymaker(context, user)
          policies = vhosts(user).flat_map(&.operator_policies.values)
          page(context, policies)
        end

        get "/api/operator-policies/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            page(context, @amqp_server.vhosts[vhost].operator_policies.values)
          end
        end

        get "/api/operator-policies/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context))
            name = URI.decode_www_form(params["name"])
            operator_policy(context, name, vhost).to_json(context.response)
          end
        end

        put "/api/operator-policies/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_administrator(context, user(context))
            name = URI.decode_www_form(params["name"])
            body = parse_body(context)
            pattern = body["pattern"]?.try &.as_s?
            definition = body["definition"]?.try &.as_h
            priority = body["priority"]?.try &.as_i? || 0
            apply_to = body["apply-to"]?.try &.as_s? || "all"
            unless pattern && definition
              bad_request(context, "Fields 'pattern' and 'definition' are required")
            end
            is_update = @amqp_server.vhosts[vhost].operator_policies[name]?
            @amqp_server.vhosts[vhost]
              .add_operator_policy(name, pattern, apply_to, definition, priority.to_i8)
            context.response.status_code = is_update ? 204 : 201
          end
        end

        delete "/api/operator-policies/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_administrator(context, user(context))
            name = URI.decode_www_form(params["name"])
            operator_policy(context, name, vhost)
            @amqp_server.vhosts[vhost].delete_operator_policy(name)
            context.response.status_code = 204
          end
        end
      end

      private def map_parameter(vhost, p)
        ParameterView.new(p, vhost)
      end

      private def param(context, parameters, id)
        parameters[id]? || not_found(context)
      end

      private def policy(context, name, vhost)
        @amqp_server.vhosts[vhost].policies[name]? || not_found(context)
      end

      private def operator_policy(context, name, vhost)
        @amqp_server.vhosts[vhost].operator_policies[name]? || not_found(context)
      end
    end
  end
end
