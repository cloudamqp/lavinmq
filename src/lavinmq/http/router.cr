require "http/server/handler"
require "http/server/context"

module LavinMQ::HTTP::Router
  include ::HTTP::Handler

  abstract struct Model
    abstract def optional?
    abstract def parse(str)
  end

  abstract struct ActionWrapper
  end

  alias Routes = Array(Route)
  alias Params = Hash(String, String)
  alias Action = Proc(::HTTP::Server::Context, Params, Model?, ::HTTP::Server::Context)

  struct ModelWrapper(T) < Model
    def initialize(@klass : T.class)
      {% raise "@klass must include JSON::Serializable" unless T <= JSON::Serializable %}
    end

    def optional? : Bool
      false
    end

    def parse(str)
      @klass.from_json(str)
    end
  end

  struct Route
    getter method, action, pattern
    getter model : Model? = nil

    def initialize(@method : String, @path : String, @action : Action, model_class)
      if mc = model_class
        @model = ModelWrapper.new mc
      end
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
    def {{method.id}}(path : String, model : Class? = nil, &block : Action)
      @_routes << Route.new({{method.upcase}}, path, block, model_class: model)
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
      route[:action].call(context, route[:params])
    else
      call_next(context)
    end
  end

  private def parse_model(model, context)
    request = context.request
    if body = request.body
      ct = request.headers["Content-Type"]?
      if ct.nil? || ct.empty? || ct == "application/json"
        model.parse body
      else
        halt(context, 415, {error: "unsupported_content_type", reason: "Unsupported content type #{ct}"})
      end
    else
      unless model.optional?
        halt(context, 400, {error: "bad_request", reason: "Request body required"})
      end
    end
  rescue e : JSON::SerializableError
    puts e.inspect
    pp e
    halt(context, 400, {error: "bad_request", reason: "Invalid value for #{e.attribute}"})
  rescue e : JSON::ParseException
    halt(context, 400, {error: "bad_request", reason: "Invalid request body"})
  end

  private def halt(context, status_code, body = nil)
    context.response.status_code = status_code
    body.try &.to_json(context.response)
    raise Controller::HaltRequest.new(body.try { |b| b[:reason] })
  end
end
