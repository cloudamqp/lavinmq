require "./router"
require "../sortable_json"

module LavinMQ
  module HTTP
    abstract class Controller
      include Router

      def initialize(@amqp_server : LavinMQ::Server)
        register_routes
      end

      private abstract def register_routes

      private def filter_values(params, iterator)
        return iterator unless raw_name = params["name"]?
        term = URI.decode_www_form(raw_name)
        if params["use_regex"]?.try { |v| v == "true" }
          iterator.select { |v| match_value(v).to_s =~ /#{term}/ }
        else
          iterator.select { |v| match_value(v).to_s.includes?(term) }
        end
      end

      protected def match_value(value)
        value[:name]? || value["name"]?
      end

      MAX_PAGE_SIZE = 10_000

      private def page(context, iterator : Iterator(SortableJSON))
        params = context.request.query_params
        page = params["page"]?.try(&.to_i) || 1
        page_size = params["page_size"]?.try(&.to_i) || 100
        if page_size > MAX_PAGE_SIZE
          context.response.status_code = 413
          {error: "payload_too_large", reason: "Max allowed page_size 10000"}.to_json(context.response)
          return context
        end
        iterator = iterator.map do |i|
          i.details_tuple
        rescue e
          {error: e.message}
        end
        all_items = filter_values(params, iterator)
        if sort_by = params.fetch("sort", nil).try &.split(".")
          sorted_items = all_items.to_a
          filtered_count = sorted_items.size
          if first_element = sorted_items.first?
            {% begin %}
              case dig(first_element, sort_by)
                {% for k in {Int32, UInt32, UInt64, Float64} %}
                when {{k.id}}
                  sorted_items.sort_by! { |i| dig(i, sort_by).as({{k.id}}) }
                {% end %}
              else
                sorted_items.sort_by! { |i| dig(i, sort_by).to_s.downcase }
              end
            {% end %}
          end
          sorted_items.reverse! if params["sort_reverse"]?.try { |s| !(s =~ /^false$/i) }
          all_items = sorted_items.each
        end
        columns = params["columns"]?.try(&.split(','))
        unless params.has_key?("page")
          JSON.build(context.response) do |json|
            items, total = array_iterator_to_json(json, all_items, columns, 0, MAX_PAGE_SIZE)
            if total > MAX_PAGE_SIZE
              Log.warn { "Result set truncated: #{items}/#{total}" }
            end
          end
          return context
        end
        JSON.build(context.response) do |json|
          json.object do
            item_count, total_count = json.field("items") do
              start = (page - 1) * page_size
              array_iterator_to_json(json, all_items, columns, start, page_size)
            end
            filtered_count ||= total_count
            json.field("filtered_count", filtered_count)
            json.field("item_count", item_count)
            json.field("page", page)
            json.field("page_count", ((total_count + page_size - 1) / page_size).to_i)
            json.field("page_size", page_size)
            json.field("total_count", total_count)
          end
        end
        context
      end

      private def dig(i : NamedTuple, keys : Array(String))
        if keys.size > 1
          nt = i[keys.first].as?(NamedTuple) || return
          dig(nt, keys[1..])
        else
          i[keys.first]? || 0
        end
      end

      private def array_iterator_to_json(json, iterator, columns : Array(String)?, start : Int, page_size : Int)
        size = 0
        total = 0
        json.array do
          iterator.each_with_index do |o, i|
            total += 1
            next if i < start || start + page_size <= i
            if columns
              json.object do
                columns.each do |col|
                  json.field col, o[col]
                rescue KeyError
                  next
                end
              end
            else
              o.to_json(json)
            end
            size += 1
          end
        end
        {size, total}
      end

      private def redirect_back(context)
        context.response.headers["Location"] = context.request.headers["Referer"]
        halt(context, 302)
      end

      private def parse_body(context)
        raise Server::ExpectedBodyError.new if context.request.body.nil?
        ct = context.request.headers["Content-Type"]?
        if ct.nil? || ct.empty? || ct == "application/json"
          body = if context.request.content_length == 0
                   JSON::Any.new({} of String => JSON::Any)
                 else
                   JSON.parse(context.request.body.not_nil!)
                 end
          if body.as_h?
            body
          else
            bad_request(context, "Input needs to be a JSON object.")
          end
        else
          raise Server::UnknownContentType.new("Unknown Content-Type: #{ct}")
        end
      rescue e : JSON::ParseException
        bad_request(context, "Malformed JSON.")
      end

      private def not_found(context, message = "Not Found")
        halt(context, 404, {error: "Object Not Found", reason: message})
      end

      private def bad_request(context, message = "Bad request")
        halt(context, 400, {error: "bad_request", reason: message})
      end

      private def access_refused(context, message = "Access refused")
        halt(context, 403, {error: "access_refused", reason: message})
      end

      private def forbidden(context, message = "Forbidden")
        halt(context, 403, {error: "forbidden", reason: message})
      end

      private def precondition_failed(context, message = "Precondition failed")
        halt(context, 412, {error: "Precondition failed", reason: message})
      end

      private def halt(context, status_code, body = nil)
        context.response.status_code = status_code
        body.try &.to_json(context.response)
        raise HaltRequest.new(body.try { |b| b[:reason] })
      end

      private def with_vhost(context, params, key = "vhost", &)
        name = URI.decode_www_form(params[key])
        if @amqp_server.vhosts[name]?
          yield name
        else
          not_found(context, "Not Found")
        end
        context
      end

      private def user(context) : User
        user = nil
        if username = context.authenticated_username?
          user = @amqp_server.users[username]?
        end
        unless user
          Log.warn { "Authorized user=#{context.authenticated_username?} not in user store" }
          access_refused(context)
        end
        user
      end

      def vhosts(user : User)
        @amqp_server.vhosts.each_value.select do |v|
          full_view_vhosts_access = user.tags.any? { |t| t.administrator? || t.monitoring? }
          amqp_access = user.permissions.has_key?(v.name)
          full_view_vhosts_access || (amqp_access && !user.tags.empty?)
        end
      end

      private def refuse_unless_vhost_access(context, user, vhost)
        return if user.tags.any? &.administrator?
        unless user.permissions.has_key?(vhost)
          Log.warn { "user=#{user.name} does not have permissions to access vhost=#{vhost}" }
          access_refused(context)
        end
      end

      private def refuse_unless_management(context, user, vhost = nil)
        unless user.tags.any? { |t| t.administrator? || t.monitoring? ||
               t.policy_maker? || t.management? }
          Log.warn { "user=#{user.name} does not have management access on vhost=#{vhost}" }
          access_refused(context)
        end
      end

      private def refuse_unless_policymaker(context, user, vhost = nil)
        unless user.tags.any? { |t| t.policy_maker? || t.administrator? }
          Log.warn { "user=#{user.name} does not have policymaker access on vhost=#{vhost}" }
          access_refused(context)
        end
      end

      private def refuse_unless_administrator(context, user : User)
        unless user.tags.any? &.administrator?
          Log.warn { "user=#{user.name} does not have administrator access" }
          access_refused(context)
        end
      end

      class HaltRequest < Exception; end
    end
  end
end
