require "./router"
require "../sortable_json"

module LavinMQ
  module HTTP
    abstract class Controller
      include Router

      # Define Log in each controller
      # source will be "lmq.http.<controller name>" without controller suffix
      macro inherited
        Log = LavinMQ::Log.for "http.{{@type.name.split("::").last.downcase.gsub(/controller$/, "").id}}"
      end

      def initialize(@amqp_server : LavinMQ::Server)
        register_routes
      end

      private abstract def register_routes

      private def page(context, iterator : Iterator(SortableJSON))
        params = context.request.query_params
        page_size = extract_page_size(context)
        search_term = extract_search_term(params)
        total_count = 0
        all_items = iterator.compact_map do |i|
          total_count += 1
          next unless i.search_match?(search_term) if search_term
          i.details_tuple
        rescue ex
          Log.warn(exception: ex) { "Could not list all items" }
          next
        end
        all_items = sort(all_items, context)
        columns = params["columns"]?.try(&.split(','))
        JSON.build(context.response) do |json|
          if page = params["page"]?.try(&.to_i)
            json.object do
              item_count, filtered_count = json.field("items") do
                start = (page - 1) * page_size
                array_iterator_to_json(json, all_items, columns, start, page_size)
              end
              json.field("filtered_count", filtered_count)
              json.field("item_count", item_count)
              json.field("page", page)
              json.field("page_count", ((filtered_count + page_size - 1) / page_size).to_i)
              json.field("page_size", page_size)
              json.field("total_count", total_count)
            end
          else
            items, total = array_iterator_to_json(json, all_items, columns, 0, MAX_PAGE_SIZE)
            if total > MAX_PAGE_SIZE
              Log.warn { "Result set truncated: #{items}/#{total}" }
            end
          end
        end
        context
      end

      private def sort(all_items, context)
        if sort_by = context.request.query_params.fetch("sort", nil).try &.split(".")
          sorted_items = all_items.to_a
          begin
            if first_element = sorted_items.first?
              case v = dig(first_element, sort_by)
              when Number
                sorted_items.sort_by! { |i| dig(i, sort_by).as(Number) }
              when String
                sorted_items.sort_by! { |i| dig(i, sort_by).as(String).downcase }
              else
                bad_request(context, "Can't sort on type #{v.class}")
              end
            end
          rescue KeyError | TypeCastError
            bad_request(context, "Sort key #{sort_by.join(".")} is not valid")
          end
          if context.request.query_params["sort_reverse"]?.try { |s| !(s =~ /^false$/i) }
            sorted_items.reverse!
          end
          sorted_items.each
        else
          all_items
        end
      end

      private def dig(tuple : NamedTuple, keys : Array(String))
        if keys.size > 1
          nt = tuple[keys.first].as?(NamedTuple) || raise KeyError.new("'#{keys.first}' is not a nested tuple")
          dig(nt, keys[1..])
        else
          tuple[keys.first] || 0
        end
      end

      # Writes the items to the json, but also counts all items by iterating the whole iterator
      # Reruns the number of number of written to the json and the total items in the iterator
      private def array_iterator_to_json(json, iterator, columns : Array(String)?, start : Int, page_size : Int) : Tuple(Int32, Int32)
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

      private def extract_page_size(context) : Int32
        page_size = context.request.query_params["page_size"]?.try(&.to_i) || 100
        if page_size > MAX_PAGE_SIZE
          halt(context, 413, {error: "payload_too_large", reason: "Max allowed page_size #{MAX_PAGE_SIZE}"})
        end
        page_size
      end

      private def extract_search_term(params)
        if raw_name = params["name"]?
          term = URI.decode_www_form(raw_name)
          if params["use_regex"]? == "true"
            Regex.new(term)
          else
            term
          end
        end
      end

      private def redirect_back(context)
        context.response.headers["Location"] = context.request.headers["Referer"]
        halt(context, 302)
      end

      private def parse_body(context) : JSON::Any
        if body = context.request.body
          ct = context.request.headers["Content-Type"]?
          if ct.nil? || ct.empty? || ct == "application/json"
            json = if context.request.content_length == 0
                     JSON::Any.new(Hash(String, JSON::Any).new)
                   else
                     JSON.parse(body)
                   end
            if json.as_h?
              json
            else
              bad_request(context, "Request body has to be a JSON object")
            end
          else
            unsupported_content_type(context, ct)
          end
        else
          bad_request(context, "Request body required")
        end
      rescue e : JSON::ParseException
        bad_request(context, "Malformed JSON")
      end

      private def not_found(context, message = "Not Found")
        halt(context, 404, {error: "Object Not Found", reason: message})
      end

      private def bad_request(context, message)
        halt(context, 400, {error: "bad_request", reason: message})
      end

      private def access_refused(context, message = "Access refused")
        halt(context, 403, {error: "access_refused", reason: message})
      end

      private def forbidden(context, message = "Forbidden")
        halt(context, 403, {error: "forbidden", reason: message})
      end

      private def precondition_failed(context, message = "Precondition failed")
        halt(context, 412, {error: "precondition_failed", reason: message})
      end

      private def unsupported_content_type(context, content_type)
        halt(context, 415, {error: "unsupported_content_type", reason: "Unsupported content type #{content_type}"})
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
        unless user.tags.any? do |t|
                 t.administrator? || t.monitoring? || t.policy_maker? || t.management?
               end
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
