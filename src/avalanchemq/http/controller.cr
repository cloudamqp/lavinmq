require "router"
require "../sortable_json"

module AvalancheMQ
  module HTTP
    abstract class Controller
      include Router

      Log = ::Log.for(self)

      def initialize(@amqp_server : AvalancheMQ::Server)
        register_routes
      end

      private abstract def register_routes

      private def query_params(context)
        context.request.query_params
      end

      private def filter_values(params, iterator)
        return iterator unless raw_name = params["name"]?
        term = URI.decode_www_form(raw_name)
        if params["use_regex"]?.try { |v| v == "true" }
          iterator.select { |v| match_value(v).to_s =~ /#{term}/ }
        else
          iterator.select { |v| match_value(v).to_s.includes?(term) }
        end
      end

      private def match_value(value)
        value[:name]? || value["name"]?
      end

      private def page(context, iterator : Iterator(SortableJSON))
        params = query_params(context)
        all_items = filter_values(params, iterator.map(&.details_tuple))
        if sort_by = params["sort"]?
          sorted_items = all_items.to_a
          filtered_count = sorted_items.size
          if first_element = sorted_items.dig?(0, sort_by)
            case first_element
            when Int32
              sorted_items.sort_by! { |i| i[sort_by].as(Int32) }
            when UInt32
              sorted_items.sort_by! { |i| i[sort_by].as(UInt32) }
            else
              sorted_items.sort_by! { |i| i[sort_by].to_s.downcase }
            end
          end
          sorted_items.reverse! if params["sort_reverse"]?.try { |s| !(s =~ /^false$/i) }
          all_items = sorted_items.each
        end
        unless params.has_key?("page")
          JSON.build(context.response) do |json|
            array_iterator_to_json(json, all_items, 0, nil)
          end
          return context
        end
        page = params["page"].to_i
        page_size = params["page_size"]?.try(&.to_i) || 1000
        start = (page - 1) * page_size
        JSON.build(context.response) do |json|
          json.object do
            item_count, total_count = json.field("items") do
              array_iterator_to_json(json, all_items, start, page_size)
            end
            filtered_count ||= total_count
            json.field("filtered_count", filtered_count)
            json.field("item_count", item_count)
            json.field("page", page)
            json.field("page_size", page_size)
            json.field("total_count", total_count)
          end
        end
        context
      end

      private def array_iterator_to_json(json, iterator, start, page_size)
        size = 0
        total = 0
        json.array do
          iterator.each_with_index do |o, i|
            raise Server::PayloadTooLarge.new if i > 10000
            total += 1
            next if i < start
            next unless page_size.nil? || i < start + page_size
            o.to_json(json)
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
        ct = context.request.headers["Content-Type"]? || nil
        if ct.nil? || ct.empty? || ct == "application/json"
          JSON.parse(context.request.body.not_nil!)
        else
          raise Server::UnknownContentType.new("Unknown Content-Type: #{ct}")
        end
      end

      private def not_found(context, message = "Not found")
        halt(context, 404, {error: "not_found", reason: message})
      end

      private def bad_request(context, message = "Bad request")
        halt(context, 400, {error: "bad_request", reason: message})
      end

      private def access_refused(context, message = "Access refused")
        halt(context, 401, {error: "access_refused", reason: message})
      end

      private def halt(context, status_code, body = nil)
        context.response.status_code = status_code
        body.try &.to_json(context.response)
        raise HaltRequest.new(body.try { |b| b[:reason] })
      end

      private def with_vhost(context, params, key = "vhost")
        vhost = URI.decode_www_form(params[key])
        if @amqp_server.vhosts[vhost]?
          yield vhost
        else
          not_found(context, "VHost #{vhost} does not exist")
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
          full_view_vhosts_access || (amqp_access && user.tags.any?)
        end
      end

      private def refuse_unless_management(context, user, vhost = nil)
        if user.tags.empty?
          Log.warn { "user=#{user.name} does not have management access on vhost=#{vhost}" }
          access_refused(context)
        end
      end

      private def refuse_unless_policymaker(context, user, vhost = nil)
        refuse_unless_management(context, user, vhost)
        unless user.tags.any? { |t| t.policy_maker? || t.administrator? }
          Log.warn { "user=#{user.name} does not have policymaker access on vhost=#{vhost}" }
          access_refused(context)
        end
      end

      private def refuse_unless_monitoring(context, user)
        refuse_unless_management(context, user)
        unless user.tags.any? { |t| t.administrator? || t.monitoring? }
          Log.warn { "user=#{user.name} does not have monitoring access" }
          access_refused(context)
        end
      end

      private def refuse_unless_administrator(context, user : User)
        refuse_unless_policymaker(context, user)
        refuse_unless_monitoring(context, user)
        unless user.tags.any? &.administrator?
          Log.warn { "user=#{user.name} does not have administrator access" }
          access_refused(context)
        end
      end

      class HaltRequest < Exception; end
    end
  end
end
