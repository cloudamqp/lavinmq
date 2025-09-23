require "http/server/handler"
require "http/server/context"

module LavinMQ::HTTP::Router
  include ::HTTP::Handler

  alias Routes = Array(Route)
  alias Params = Hash(String, String)
  alias ActionBlock = Proc(::HTTP::Server::Context, Params, ::HTTP::Server::Context)

  abstract struct Action
    abstract def act(context : ::HTTP::Server::Context, params : Params) : ::HTTP::Server::Context

    private def halt(context, status_code, body = nil)
      context.response.status_code = status_code
      body.try &.to_json(context.response)
      raise Controller::HaltRequest.new(body.try { |b| b[:reason] })
    end
  end

  struct ActionSimple < Action
    def initialize(@target : ActionBlock)
    end

    def act(context : ::HTTP::Server::Context, params : Params) : ::HTTP::Server::Context
      @target.call(context, params)
    end
  end

  struct ActionWithModel(T) < Action
    def initialize(@target : Proc(::HTTP::Server::Context, Params, T, ::HTTP::Server::Context))
    end

    def act(context : ::HTTP::Server::Context, params : Params) : ::HTTP::Server::Context
      request = context.request
      model = if body = request.body
                ct = request.headers["Content-Type"]?
                if ct.nil? || ct.empty? || ct == "application/json"
                  T.from_json(body)
                else
                  halt(context, 415, {error: "unsupported_content_type", reason: "Unsupported content type #{ct}"})
                end
              else
                {% if T.nilable? %}
                  nil
                {% else %}
                  halt(context, 400, {error: "bad_request", reason: "Request body required"})
                {% end %}
              end
      @target.call(context, params, model)
    rescue e : JSON::SerializableError
      halt(context, 400, {error: "bad_request", reason: "Invalid value for #{e.attribute}"})
    rescue e : JSON::ParseException
      halt(context, 400, {error: "bad_request", reason: "Invalid request body"})
    end
  end

  struct Route
    getter method, action, pattern

    def initialize(@method : String, @path : String, @action : Action)
      path = @path.split("/").map do |part|
        case part[0]?
        when ':' then "(?<#{Regex.escape(part[1..])}>[^/]*)"
        when '*' then "(?<#{Regex.escape(part[1..])}>.*)"
        else          Regex.escape(part)
        end
      end.join("/")
      @pattern = Regex.new("^/#{method}#{path}$")
    end
  end

  @_routes = Routes.new

  {% for method in %w[delete get head options patch post put] %}
    def {{method.id}}(path : String, &block : ActionBlock)
      @_routes << Route.new({{method.upcase}}, path, ActionSimple.new(block))
    end
    def {{method.id}}(path : String, model : T.class,
        &block : Proc(::HTTP::Server::Context, Params, T, ::HTTP::Server::Context)) forall T
    @_routes << Route.new({{method.upcase}}, path, ActionWithModel(T).new(block))
    end
    {% end %}

  # def find_route(method, path)
  def find_route(context)
    method = context.request.method
    path = context.request.path
    search_path = "/#{method}/#{path.strip('/')}"
    @_routes.each do |r|
      if res = r.pattern.match(search_path)
        if model = r.model
          model = parse_model(model, context)
        end
        ret = {
          action: r.action,
          # reject and transform_value to go from Hash(String, String | Nil) to Hash(String, String)
          params: res.named_captures.reject! { |_k, v| v.nil? }.transform_values &.to_s,
          model:  model,
        }

        return ret
      end
    end
    nil
  end

  def call(context)
    # if route = find_route(context.request.method, context.request.path)
    if route = find_route(context)
      route[:action].act(context, route[:params])
    else
      call_next(context)
    end
  end
end
