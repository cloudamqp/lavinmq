require "http/server"
require "json"
require "logger"
require "router"
require "./error"

module AvalancheMQ

  class ApiDefaultsHandler
    include HTTP::Handler
    def call(context)
      if context.request.path.starts_with?("/api/")
        context.response.content_type = "application/json"
      end
      call_next(context)
    end
  end

  class ApiErrorHandler
    include HTTP::Handler

    def initialize(@log : Logger)
    end

    def call(context)
      call_next(context)
    rescue ex : HTTPServer::UnknownContentType
      context.response.content_type = "text/plain"
      context.response.status_code = 415
      context.response.print ex.message
    rescue ex : HTTPServer::NotFoundError
      not_found(context, ex.message)
    rescue ex : JSON::Error | ArgumentError
      @log.error "path=#{context.request.path} error=#{ex.inspect}"
      context.response.status_code = 400
      { error: "#{ex.inspect}" }.to_json(context.response)
    end

    def not_found(context, message = nil)
      context.response.content_type = "text/plain"
      context.response.status_code = 404
      context.response.print "Not found\n"
      context.response.print message
    end
  end
  class HTTPServer
    include Router

    @log : Logger
    def initialize(@amqp_server : AvalancheMQ::Server, @port : Int32)
      @log = @amqp_server.log.dup
      @log.progname = "HTTP(#{port})"
    end

    def register_routes
      get "/api/connections" do |context, _params|
        @amqp_server.connections.to_json(context.response)
        context
      end
      get "/api/exchanges" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.exchanges.values }.to_json(context.response)
        context
      end
      get "/api/queues" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.queues.values }.to_json(context.response)
        context
      end
      get "/api/policies" do |context, _params|
        @amqp_server.vhosts.flat_map { |v| v.policies.values }.to_json(context.response)
        context
      end
      get "/api/vhosts" do |context, _params|
        @amqp_server.vhosts.to_json(context.response)
        context
      end
      get "/" do |context, _params|
        context.response.content_type = "text/plain"
        context.response.print "AvalancheMQ"
        context
      end

      post "/api/parameters" do |context, _params|
        p = Parameter.from_json context.request.body.not_nil!
        @amqp_server.add_parameter p
        context
      end
      post "/api/policies" do |context, _params|
        p = Policy.from_json context.request.body.not_nil!
        vhost = @amqp_server.vhosts[p.vhost]? || nil
        raise NotFoundError.new("No vhost named #{p.vhost}") unless vhost
        vhost.add_policy(p)
        context
      end
      post "/api/vhosts" do |context, _params|
        body = parse_body(context)
        @amqp_server.vhosts.create(body["name"].as_s)
        context
      end
      post "/api/definitions" do |context, _params|
        body = parse_body(context)
        import_definitions(body)
        context
      end

      delete "/api/policies" do |context, _params|
        body = parse_body(context)
        vhost = @amqp_server.vhosts[body["vhost"].as_s]?
          raise NotFoundError.new("No vhost named #{body["vhost"].as_s}") unless vhost
        vhost.delete_policy(body["name"].as_s)
        context
      end
      delete "/api/vhosts" do |context, _params|
        body = parse_body(context)
        @amqp_server.vhosts.delete(body["name"].as_s)
        context
      end
    end

    def parse_body(context)
      raise ExpectedBodyError.new if context.request.body.nil?
      ct = context.request.headers["Content-Type"]? || nil
      if ct.nil? || ct.empty? || ct == "application/json"
        JSON.parse(context.request.body.not_nil!)
      else
        raise UnknownContentType.new("Unknown Content-Type: #{ct}")
      end
    end

    def listen
      register_routes
      @http = HTTP::Server.new(@port, [ApiDefaultsHandler.new,
                                       ApiErrorHandler.new(@log),
                                       route_handler])
      server = @http.not_nil!.bind
      @log.info "Listening on #{server.local_address}"
      @http.not_nil!.listen
    end

    def close
      @http.try &.close
    end

    private def import_definitions(body)
      if users = body["users"]? || nil
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
      if vhosts = body["vhosts"]? || nil
        vhosts.each do |v|
          name = v["name"].as_s
          @amqp_server.vhosts.create name
        end
      end
      if queues = body["queues"]? || nil
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
      if exchanges = body["exchanges"]? || nil
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
      if bindings = body["bindings"]? || nil
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

      if permissions = body["permissions"]? || nil
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
    end

    class NotFoundError < Exception; end
    class ExpectedBodyError < ArgumentError; end
    class UnknownContentType < Exception; end
  end
end
