require "./config"
require "./api"
require "./broker"
require "./endpoint"
require "./scenario"
require "./scenarios/*"

module ShovelTest
  # Runs every scenario in order against one vhost and reports.
  class Suite
    SCENARIOS = [
      RejectPublishOverflow,
      QueueLengthRun,
      HttpConfirmed,
      HttpRetry,
      HttpReject,
      HttpAbort,
      PauseResume,
    ]

    def self.main : Nil
      STDOUT.sync = true
      exit(new(Config.parse).run ? 0 : 1)
    end

    def initialize(@cfg : Config)
      @api = Api.new(@cfg)
      @endpoint = Endpoint.new(@cfg)
      @ctx = Context.new(@cfg, @api, Broker.new(@cfg), @endpoint)
    end

    # True when every selected scenario passed.
    def run : Bool
      log "target #{@cfg.http_base} (LavinMQ #{@api.server_version || "?"}) vhost=#{@cfg.vhost}"
      created_vhost = prepare_vhost
      @endpoint.start
      log "HTTP endpoint at #{@endpoint.url("/")}"

      scenarios = SCENARIOS.map(&.new(@ctx))
      if only = @cfg.only
        scenarios.select! &.slug.includes?(only)
      end
      failed = scenarios.reject { |scenario| run_scenario(scenario) }

      log "#{scenarios.size - failed.size}/#{scenarios.size} scenarios passed"
      failed.each { |scenario| log "  failed: #{scenario.slug}" }
      failed.empty?
    ensure
      @endpoint.stop
      teardown_vhost if created_vhost && !@cfg.keep?
    end

    private def run_scenario(scenario : Scenario) : Bool
      log "RUN  #{scenario.slug}"
      started = Time.instant
      scenario.run
      log "PASS #{scenario.slug} (#{(Time.instant - started).total_seconds.round(1)}s)"
      true
    rescue ex : Failure
      log "FAIL #{scenario.slug}: #{ex.message}"
      diagnostics
      false
    rescue ex
      log "ERROR #{scenario.slug}: #{ex.class}: #{ex.message}"
      diagnostics
      false
    ensure
      begin
        scenario.cleanup
      rescue ex
        log "cleanup of #{scenario.slug} failed: #{ex.message}"
      end
    end

    # Returns true when the vhost was created here (and so is ours to delete).
    private def prepare_vhost : Bool
      if @api.vhost_exists?
        raise "vhost #{@cfg.vhost} already exists; pass --vhost to reuse it" unless @cfg.vhost_given?
        return false
      end
      @api.create_vhost
      log "created vhost #{@cfg.vhost}"
      true
    end

    private def teardown_vhost : Nil
      @api.delete_vhost
      log "deleted vhost #{@cfg.vhost}"
    rescue ex
      log "could not delete vhost #{@cfg.vhost}: #{ex.message}"
    end

    private def diagnostics : Nil
      log "state of vhost #{@cfg.vhost}:"
      puts @api.queues_summary
      puts @api.shovels_summary
    rescue ex
      log "could not fetch diagnostics: #{ex.message}"
    end

    private def log(message : String) : Nil
      puts "#{Time.local.to_s("%H:%M:%S")} #{message}"
    end
  end
end
