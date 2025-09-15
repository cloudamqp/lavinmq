require "http/server/handler"
require "http/server/context"
require "uri"

module LavinMQ::HTTP::Router
  include ::HTTP::Handler

  alias Routes = Array(Route)
  alias Action = Proc(::HTTP::Server::Context, Params, ::HTTP::Server::Context)
  alias Params = Hash(String, String)

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
    def {{method.id}}(path : String, &block : Action)
      @_routes << Route.new({{method.upcase}}, path, block)
    end
  {% end %}

  def find_route(method, path)
    search_path = "/#{method}/#{path.strip('/')}"
    @_routes.each do |r|
      if res = r.pattern.match(search_path)
        return {
          action: r.action,
          # reject and transform_value to go from Hash(String, String | Nil) to Hash(String, String)
          params: res.named_captures.reject! { |_k, v| v.nil? }.transform_values do |value|
            URI.decode(value.to_s)
          end,
        }
      end
    end
    nil
  end

  def call(context)
    if route = find_route(context.request.method, context.request.path)
      route[:action].call(context, route[:params])
    else
      call_next(context)
    end
  end
end
