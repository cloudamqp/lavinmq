require "http/server/handler"
require "http/server/context"
require "string_scanner"

module LavinMQ::HTTP::Router
  include ::HTTP::Handler

  alias Params = Hash(String, String)
  alias Routes = Array(Route)
  alias Action = Proc(::HTTP::Server::Context, Params, ::HTTP::Server::Context)

  abstract struct Fragment
    def initialize(@value : String)
    end

    def match?(path : String, params : Params) : Bool
      match?(StringScanner.new(path), params)
    end

    abstract def match?(path : StringScanner, params : Params) : Bool
  end

  struct StaticFragment < Fragment
    FRAGMENT_PATTERN = %r{[^/]*}

    def match?(path : StringScanner, params : Params) : Bool
      path_fragment = path.scan FRAGMENT_PATTERN
      path_fragment == @value
    end
  end

  struct ParamFragment < Fragment
    FRAGMENT_PATTERN = %r{[^/]*}

    def match?(path : StringScanner, params : Params) : Bool
      if path_fragment = path.scan FRAGMENT_PATTERN
        params[@value] = path_fragment
        return true
      end
      false
    end
  end

  struct RestFragment < Fragment
    def match?(path : StringScanner, params : Params) : Bool
      params[@value] = path.rest
      path.terminate
      true
    end
  end

  struct Fragments
    FRAGMENT_SEPERATOR = %r{/}

    def self.create(path) : Fragments
      path = path.lstrip '/'
      new(path.split('/').map do |value|
        case value[0]?
        when ':'
          ParamFragment.new(value[1..])
        when '*'
          RestFragment.new(value[1..])
        else
          StaticFragment.new(value)
        end
      end)
    end

    def initialize(@fragments : Array(Fragment))
    end

    def match?(path : String, params : Params) : Bool
      match?(StringScanner.new(path), params)
    end

    def match?(path : StringScanner, params : Params) : Bool
      fragments = @fragments.each
      until path.eos?
        fragment = fragments.next
        case fragment
        when Iterator::Stop then return false
        else
          path.skip FRAGMENT_SEPERATOR
          return false unless fragment.match?(path, params)
        end
      end
      # It's a match if we've matched against all fragments
      fragments.next == fragments.stop
    end
  end

  struct Route
    getter method, fragments, action

    def initialize(@method : String, @path : String, @action : Action)
      @fragments = Fragments.create(@path)
    end
  end

  @_routes = Routes.new

  {% for method in %w[delete get options patch post put] %}
    def {{method.id}}(path : String, &block : Action)
      @_routes << Route.new({{method.upcase}}, path, block)
    end
  {% end %}

  private def find_route(method, path)
    params = Params.new
    path = StringScanner.new(path)
    @_routes.each do |route|
      next unless route.method == method
      params.clear
      path.reset
      if route.fragments.match?(path, params)
        return {params: params, action: route.action}
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
