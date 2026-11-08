require "./router"
require "../sortable_json"

module LavinMQ
  module HTTP
    abstract class Controller
      include Router

      # Define Log in each controller
      # source will be "lmq.http.<controller name>" without controller suffix
      macro inherited
        Log = LavinMQ::Log.for "http.{{ @type.name.split("::").last.downcase.gsub(/controller$/, "").id }}"
      end

      def initialize(@server : LavinMQ::Server)
        register_routes
      end

      private abstract def register_routes

      private def page(context, items : Array(SortableJSON))
        params = context.request.query_params
        page_size = extract_page_size(context)
        search_term = extract_search_term(params)
        states = extract_state_filter(context)
        total_count = items.size
        # Details are built per item written out, or sorted on; one array of all of them is a multi-MB
        # pointerful allocation the GC can fail to place with many fibers alive
        if search_term || states
          items = items.select do |i|
            (search_term.nil? || i.search_match?(search_term)) &&
              (states.nil? || i.state_match?(states))
          end
        end
        items = sort(items, context)
        columns = params["columns"]?.try(&.split(','))
        JSON.build(context.response.output) do |json|
          if page = params["page"]?.try(&.to_i)
            json.object do
              item_count, filtered_count = json.field("items") do
                start = (page - 1) * page_size
                array_iterator_to_json(json, items, columns, start, page_size)
              end
              json.field("filtered_count", filtered_count)
              json.field("item_count", item_count)
              json.field("page", page)
              json.field("page_count", ((filtered_count + page_size - 1) / page_size).to_i)
              json.field("page_size", page_size)
              json.field("total_count", total_count)
            end
          else
            written, total = array_iterator_to_json(json, items, columns, 0, MAX_PAGE_SIZE)
            if total > MAX_PAGE_SIZE
              Log.warn { "Result set truncated: #{written}/#{total}" }
            end
          end
        end
        context
      end

      private def sort(all_items, context)
        if sort_by = context.request.query_params.fetch("sort", nil).try &.split(".")
          begin
            sort_by_key!(all_items, sort_by, context)
          rescue KeyError | TypeCastError
            bad_request(context, "Sort key #{sort_by.join(".")} is not valid")
          end
          if context.request.query_params["sort_reverse"]?.try { |s| !(s =~ /^false$/i) }
            all_items.reverse!
          end
        end
        all_items
      end

      private def sort_by_key!(sorted_items, sort_by, context)
        # Find first non-nil value to determine the sort type
        sample_value = sorted_items.each do |item|
          v = sort_key(item, sort_by)
          break v unless v.nil?
        end
        case sample_value
        when Number
          sorted_items.sort_by! { |i| sort_key(i, sort_by).as?(Number) || 0 }
        when String
          sorted_items.sort_by! { |i| (sort_key(i, sort_by).as?(String) || "").downcase }
        when Bool
          sorted_items.sort_by! { |i| sort_key(i, sort_by).as?(Bool) ? 1 : 0 }
        when QueueState
          sorted_items.sort_by! { |i| sort_key(i, sort_by).as?(QueueState) || QueueState::Closed }
        when Nil
          nil # all values are nil, nothing to sort
        else
          bad_request(context, "Can't sort on type #{sample_value.class}")
        end
      end

      private def sort_key(item : SortableJSON, keys : Array(String))
        if tuple = details(item)
          dig(tuple, keys)
        end
      end

      # An item that can't be described any more (e.g. deleted meanwhile) is left out
      private def details(item : SortableJSON)
        item.details_tuple
      rescue ex
        Log.warn(exception: ex) { "Could not list all items" }
        nil
      end

      private def dig(tuple : NamedTuple, keys : Array(String))
        if keys.size > 1
          nt = tuple[keys.first].as?(NamedTuple) || raise KeyError.new("'#{keys.first}' is not a nested tuple")
          dig(nt, keys[1..])
        else
          tuple[keys.first]?
        end
      end

      # Writes the page from *start* and returns {written, total}; an item that can't be
      # described is neither written nor counted, the next one takes its slot
      private def array_iterator_to_json(json, items, columns : Array(String)?, start : Int, page_size : Int) : Tuple(Int32, Int32)
        written = 0
        total = 0
        json.array do
          items.each do |o|
            if total >= start && written < page_size
              tuple = details(o) || next
              if columns
                json.object do
                  columns.each do |col|
                    json.field col, tuple[col]
                  rescue KeyError
                    next
                  end
                end
              else
                tuple.to_json(json)
              end
              written += 1
            end
            total += 1
          end
        end
        {written, total}
      end

      private def extract_page_size(context) : Int32
        page_size = context.request.query_params["page_size"]?.try(&.to_i) || 100
        if page_size > MAX_PAGE_SIZE
          halt(context, 413, {error: "payload_too_large", reason: "Max allowed page_size #{MAX_PAGE_SIZE}"})
        end
        page_size
      end

      private def extract_search_term(params)
        if term = params["name"]?
          if params["use_regex"]? == "true"
            Regex.new(term)
          else
            term
          end
        end
      end

      private def extract_state_filter(context) : Array(QueueState)?
        return unless param = context.request.query_params["state"]?
        param.split(',').map do |s|
          QueueState.parse?(s) || bad_request(context, "Invalid queue state '#{s}'")
        end
      end

      private def redirect_back(context)
        context.response.headers["Location"] = context.request.headers["Referer"]
        halt(context, 302)
      end

      private def parse_body(context) : JSON::Any
        request = context.request
        # A POST or PUT without a body is an empty object. Before Crystal 1.22
        # such requests got a Content-Length of 0, now they have no body.
        if request.content_length == 0 || (request.body.nil? && request.method.in?("POST", "PUT"))
          return JSON::Any.new(Hash(String, JSON::Any).new)
        end
        if body = request.body
          ct = request.headers["Content-Type"]?
          if ct.nil? || ct.empty? || ct == "application/json"
            json = JSON.parse(body)
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
      rescue JSON::ParseException
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
        body.try &.to_json(context.response.output)
        raise HaltRequest.new(body.try { |b| b[:reason] })
      end

      private def with_vhost(context, params, *, vhost_key = "vhost", &)
        name = params[vhost_key]?
        if name && (vhost = @server.vhosts[name]?)
          refuse_unless_vhost_access(context, user(context), vhost)
          yield vhost
        elsif user(context).tags.any? &.administrator?
          not_found(context, "Vhost #{name} does not exist")
        else
          access_refused(context)
        end
        context
      end

      private def user(context) : Auth::BaseUser
        unless user = context.user # Should it be possible to end up here with nil user?
          Log.warn { "No authorized user for request path=#{context.request.path}" }
          access_refused(context)
        end
        user
      end

      def vhosts(user : Auth::BaseUser) : Array(VHost)
        @server.vhosts.values.select do |v|
          full_view_vhosts_access = user.tags.any? { |t| t.administrator? || t.monitoring? }
          amqp_access = user.permissions.has_key?(v.name)
          full_view_vhosts_access || (amqp_access && !user.tags.empty?)
        end
      end

      private def refuse_unless_vhost_access(context, user, vhost : VHost)
        return if user.tags.any? &.administrator?
        unless user.permissions.has_key?(vhost.name)
          Log.warn { "user=#{user.name} does not have permissions to access vhost=#{vhost.name}" }
          access_refused(context)
        end
      end

      private def refuse_unless_management(context, user, vhost : VHost? = nil)
        unless user.tags.any? do |t|
                 t.administrator? || t.monitoring? || t.policy_maker? || t.management?
               end
          Log.warn { "user=#{user.name} does not have management access on vhost=#{vhost.try(&.name)}" }
          access_refused(context)
        end
      end

      private def refuse_unless_policymaker(context, user, vhost : VHost? = nil)
        unless user.tags.any? { |t| t.policy_maker? || t.administrator? }
          Log.warn { "user=#{user.name} does not have policymaker access on vhost=#{vhost.try(&.name)}" }
          access_refused(context)
        end
      end

      private def refuse_unless_administrator(context, user : Auth::BaseUser)
        unless user.tags.any? &.administrator?
          Log.warn { "user=#{user.name} does not have administrator access" }
          access_refused(context)
        end
      end

      class HaltRequest < Exception; end
    end
  end
end
