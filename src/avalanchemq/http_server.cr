require "http/server"
require "json"
require "logger"
require "./error"

module AvalancheMQ
  class HTTPServer
    @log : Logger
    def initialize(@amqp_server : AvalancheMQ::Server, port)
      @log = @amqp_server.log.dup
      @log.progname = "HTTP(#{port})"
      @http = HTTP::Server.new(port) do |context|
        context.response.content_type = "application/json"
        case context.request.method
        when "GET"
          get(context)
        when "POST"
          post(context)
        when "DELETE"
          delete(context)
        else
          context.response.content_type = "text/plain"
          context.response.status_code = 405
          context.response.print "Method not allowed"
        end
      rescue ex : UnknownContentType
        context.response.content_type = "text/plain"
        context.response.status_code = 415
        context.response.print ex.message
      rescue ex : NotFoundError
        not_found(context, ex.message)
      rescue ex : JSON::Error | ArgumentError
        @log.error "path=#{context.request.path} error=#{ex.inspect}"
        context.response.status_code = 400
        { error: "#{ex.inspect}" }.to_json(context.response)
      end
    end

    def get(context)
      case context.request.path
      when "/api/connections"
        @amqp_server.connections.to_json(context.response)
      when "/api/exchanges"
        @amqp_server.vhosts.flat_map { |v| v.exchanges.values }.to_json(context.response)
      when "/api/queues"
        @amqp_server.vhosts.flat_map { |v| v.queues.values }.to_json(context.response)
      when "/api/policies"
        @amqp_server.vhosts.flat_map { |v| v.policies.values }.to_json(context.response)
      when "/api/vhosts"
        @amqp_server.vhosts.to_json(context.response)
      when "/"
        context.response.content_type = "text/plain"
        context.response.print "AvalancheMQ"
      else
        not_found(context)
      end
    end

    def post(context)
      case context.request.path
      when "/api/parameters"
        body = parse_body(context)
        @amqp_server.add_parameter(Parameter.from_json(body))
      when "/api/policies"
        body = parse_body(context)
        vhost = @amqp_server.vhosts[body["vhost"].as_s]?
        raise NotFoundError.new("No vhost named #{body["vhost"].as_s}") unless vhost
        vhost.add_policy(Policy.from_json(body))
      when "/api/vhosts"
        body = parse_body(context)
        @amqp_server.vhosts.create(body["name"].as_s)
      when "/api/definitions"
        body = parse_body(context)
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
      else
        not_found(context)
      end
    end

    def delete(context)
      case context.request.path
      when "/api/policies"
        body = parse_body(context)
        vhost = @amqp_server.vhosts[body["vhost"].as_s]?
          raise NotFoundError.new("No vhost named #{body["vhost"].as_s}") unless vhost
        vhost.delete_policy(body["name"].as_s)
      when "/api/vhosts"
        body = parse_body(context)
        @amqp_server.vhosts.delete(body["name"].as_s)
      else
        not_found(context)
      end
    end

    def not_found(context, message = nil)
      context.response.content_type = "text/plain"
      context.response.status_code = 404
      context.response.print "Not found\n"
      context.response.print message
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
      server = @http.bind
      @log.info "Listening on #{server.local_address}"
      @http.listen
    end

    def close
      @http.close
    end

    class NotFoundError < Exception; end
    class ExpectedBodyError < ArgumentError; end
    class UnknownContentType < Exception; end
  end
end
