require "./config"
require "./api"
require "./broker"
require "./endpoint"

module ShovelTest
  # The value of a shovel parameter, as sent to the management API.
  alias ShovelConfig = Hash(String, String | Int32 | Array(String))

  # An expectation that did not hold.
  class Failure < Exception; end

  record Context, cfg : Config, api : Api, broker : Broker, endpoint : Endpoint

  # One observable behaviour of the shovel, checked end to end. Every queue,
  # shovel and endpoint path a scenario creates is prefixed with its slug, so
  # scenarios do not see each other's traffic. Resources are removed in
  # `cleanup`, whatever the result.
  abstract class Scenario
    @shovels = [] of String
    @queues = [] of String

    def initialize(@ctx : Context)
    end

    # A kebab-case name; also the prefix of everything the scenario creates.
    abstract def slug : String

    # Raises Failure when an expectation does not hold.
    abstract def run : Nil

    def cleanup : Nil
      @shovels.each { |name| api.delete_shovel(name) }
      @queues.each { |name| api.delete_queue(name) }
    end

    private def cfg : Config
      @ctx.cfg
    end

    private def api : Api
      @ctx.api
    end

    private def broker : Broker
      @ctx.broker
    end

    private def queue(suffix : String, arguments : Hash = {} of String => String) : String
      name = "#{slug}.#{suffix}"
      api.declare_queue(name, arguments)
      @queues << name
      name
    end

    private def shovel(suffix : String, config : ShovelConfig) : String
      name = "#{slug}.#{suffix}"
      api.create_shovel(name, config)
      @shovels << name
      name
    end

    # A shovel parameter reading from `src_queue` on this broker. Destination
    # keys are given as `dest_uri:`, `dest_queue:` etc. and become kebab-case.
    private def shovel_config(src_queue : String, **fields) : ShovelConfig
      config = ShovelConfig{
        "src-uri"         => cfg.amqp_url,
        "src-queue"       => src_queue,
        "reconnect-delay" => 1,
      }
      fields.each { |key, value| config[key.to_s.tr("_", "-")] = value }
      config
    end

    # The HTTP endpoint URL for `path`, namespaced to this scenario.
    private def endpoint_url(path : String) : String
      @ctx.endpoint.url("/#{slug}#{path}")
    end

    private def hits(path : String) : Int32
      @ctx.endpoint.hits("/#{slug}#{path}")
    end

    private def status(name : String) : Api::ShovelStatus
      api.shovel(name) || raise Failure.new("shovel #{name} is no longer known to the broker")
    end

    # Polls until the block holds; fails after the configured timeout.
    private def eventually(what : String, timeout : Time::Span = cfg.timeout, &block : -> Bool) : Nil
      deadline = Time.instant + timeout
      until block.call
        raise Failure.new("timed out after #{timeout.total_seconds}s waiting for #{what}") if Time.instant > deadline
        sleep 100.milliseconds
      end
    end

    # The block must hold at every poll for the whole `hold` window.
    private def steady(what : String, hold : Time::Span, &block : -> Bool) : Nil
      deadline = Time.instant + hold
      while Time.instant < deadline
        raise Failure.new("#{what} did not hold for #{hold.total_seconds}s") unless block.call
        sleep 200.milliseconds
      end
    end

    private def expect(condition : Bool, what : String) : Nil
      raise Failure.new(what) unless condition
    end
  end
end
