require "../spec_helper"

describe LavinMQ::ScheduledJob::Store do
  describe ".validate_config!" do
    it "accepts a valid config" do
      cfg = JSON.parse({
        cron:          "*/5 * * * *",
        exchange:      "",
        "routing-key": "task",
        body:          "hi",
      }.to_json)
      LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
    end

    it "rejects a missing cron" do
      cfg = JSON.parse({"routing-key": "x", body: "y"}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /cron/i) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects an invalid cron expression" do
      cfg = JSON.parse({cron: "not-a-cron", "routing-key": "x", body: "y"}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /cron/i) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects a missing routing-key" do
      cfg = JSON.parse({cron: "* * * * *", body: "y"}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /routing-key/i) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects a missing body" do
      cfg = JSON.parse({cron: "* * * * *", "routing-key": "x"}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /body/i) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "accepts 'schedule' as an alias for 'cron'" do
      cfg = JSON.parse({
        schedule:      "*/5 * * * *",
        "routing-key": "x",
        body:          "y",
      }.to_json)
      LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
    end

    it "accepts 'routing_key' as an alias for 'routing-key'" do
      cfg = JSON.parse({
        cron:        "* * * * *",
        routing_key: "x",
        body:        "y",
      }.to_json)
      LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
    end

    it "rejects a non-object config" do
      cfg = JSON.parse(%q(["not", "a", "hash"]))
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /JSON object/) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects an empty cron string" do
      cfg = JSON.parse({cron: "", "routing-key": "x", body: "y"}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /must not be empty/) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects an empty routing-key" do
      cfg = JSON.parse({cron: "* * * * *", "routing-key": "", body: "y"}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /must not be empty/) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "allows an empty body" do
      cfg = JSON.parse({cron: "* * * * *", "routing-key": "x", body: ""}.to_json)
      LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
    end

    it "rejects wrong type for cron" do
      cfg = JSON.parse({cron: 5, "routing-key": "x", body: "y"}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /'cron' must be a string/) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects wrong type for routing-key" do
      cfg = JSON.parse({cron: "* * * * *", "routing-key": true, body: "y"}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /'routing-key' must be a string/) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects wrong type for body" do
      cfg = JSON.parse({cron: "* * * * *", "routing-key": "x", body: {a: 1}}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /'body' must be a string/) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects wrong type for exchange" do
      cfg = JSON.parse({cron: "* * * * *", exchange: 1, "routing-key": "x", body: "y"}.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /'exchange' must be a string/) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects wrong type for content-type" do
      cfg = JSON.parse({
        cron: "* * * * *", "routing-key": "x", body: "y", "content-type": 7,
      }.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /'content-type' must be a string/) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "rejects wrong type for headers" do
      cfg = JSON.parse({
        cron: "* * * * *", "routing-key": "x", body: "y", headers: "nope",
      }.to_json)
      expect_raises(LavinMQ::ScheduledJob::ConfigError, /'headers' must be a JSON object/) do
        LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
      end
    end

    it "accepts the empty string for exchange (default exchange)" do
      cfg = JSON.parse({cron: "* * * * *", exchange: "", "routing-key": "x", body: "y"}.to_json)
      LavinMQ::ScheduledJob::Store.validate_config!(cfg, nil)
    end
  end

  describe "debounced state persistence" do
    it "does not write to disk on every fire (only on explicit flush or interval)" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        cfg = JSON.parse({
          cron: "0 0 1 1 *", "routing-key": "k", body: "b",
        }.to_json)
        runner = vhost.scheduled_jobs.create("debounced", cfg)
        # State file should not exist before any fire
        File.exists?(File.join(vhost.data_dir, "scheduled_jobs_state.json")).should be_false
        runner.run_now
        wait_for { runner.run_count > 0 }
        # Immediately after fire, no synchronous write happened
        File.exists?(File.join(vhost.data_dir, "scheduled_jobs_state.json")).should be_false
        vhost.scheduled_jobs.flush!
        # After explicit flush, file exists and contains the stats
        path = File.join(vhost.data_dir, "scheduled_jobs_state.json")
        File.exists?(path).should be_true
        File.read(path).includes?(%("run_count":1)).should be_true
        vhost.scheduled_jobs.delete("debounced")
      end
    end

    it "coalesces many fires into a single flush" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        cfg = JSON.parse({
          cron: "0 0 1 1 *", "routing-key": "k", body: "b",
        }.to_json)
        runner = vhost.scheduled_jobs.create("coalesce", cfg)
        100.times { runner.run_now; Fiber.yield }
        wait_for { runner.run_count >= 100 }
        vhost.scheduled_jobs.flush!
        path = File.join(vhost.data_dir, "scheduled_jobs_state.json")
        File.read(path).includes?(%("run_count":#{runner.run_count})).should be_true
        vhost.scheduled_jobs.delete("coalesce")
      end
    end

    it "delete is synchronously persisted" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        cfg = JSON.parse({
          cron: "0 0 1 1 *", "routing-key": "k", body: "b",
        }.to_json)
        runner = vhost.scheduled_jobs.create("del-sync", cfg)
        runner.run_now
        wait_for { runner.run_count > 0 }
        vhost.scheduled_jobs.delete("del-sync")
        path = File.join(vhost.data_dir, "scheduled_jobs_state.json")
        File.exists?(path).should be_true
        File.read(path).includes?("del-sync").should be_false
      end
    end

    it "close flushes pending stats" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        # Use a private store so we can close it without tearing down the vhost
        store = LavinMQ::ScheduledJob::Store.new(vhost)
        cfg = JSON.parse({
          cron: "0 0 1 1 *", "routing-key": "k", body: "b",
        }.to_json)
        runner = store.create("flush-on-close", cfg)
        runner.run_now
        wait_for { runner.run_count > 0 }
        store.close
        path = File.join(vhost.data_dir, "scheduled_jobs_state.json")
        File.read(path).includes?(%("run_count":1)).should be_true
      end
    end
  end

  describe "create() also enforces validation" do
    it "raises ConfigError on missing body when called directly" do
      with_amqp_server do |s|
        cfg = JSON.parse({cron: "* * * * *", "routing-key": "x"}.to_json)
        expect_raises(LavinMQ::ScheduledJob::ConfigError, /body/) do
          s.vhosts["/"].scheduled_jobs.create("bad", cfg)
        end
        s.vhosts["/"].scheduled_jobs.has_key?("bad").should be_false
      end
    end
  end

  describe "run_now / fire" do
    it "publishes a message into the target exchange" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("scheduled-target-q", durable: false, auto_delete: false)
        vhost.bind_queue("scheduled-target-q", "amq.topic", "scheduled.test")

        cfg = JSON.parse({
          cron:          "0 0 1 1 *", # never within the test window
          exchange:      "amq.topic",
          "routing-key": "scheduled.test",
          body:          "payload",
        }.to_json)
        runner = vhost.scheduled_jobs.create("test-job", cfg)
        runner.run_now.should be_true

        wait_for { runner.run_count > 0 }
        runner.run_count.should eq 1
        vhost.queue("scheduled-target-q").message_count.should eq 1
        vhost.scheduled_jobs.delete("test-job")
      end
    end
  end

  describe "create with alternate field names" do
    it "accepts schedule/routing_key aliases and amq.default" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.declare_queue("alt-target-q", durable: false, auto_delete: false)

        cfg = JSON.parse({
          schedule:    "0 0 1 1 *",
          exchange:    "amq.default", # normalized to "" (default exchange)
          routing_key: "alt-target-q",
          body:        "{}",
        }.to_json)
        runner = vhost.scheduled_jobs.create("alt-job", cfg)
        runner.exchange.should eq ""
        runner.routing_key.should eq "alt-target-q"
        runner.run_now.should be_true
        wait_for { runner.run_count > 0 }
        vhost.queue("alt-target-q").message_count.should eq 1
        vhost.scheduled_jobs.delete("alt-job")
      end
    end
  end

  describe "vhost parameter wiring" do
    it "creating a 'scheduled-job' parameter creates a runner" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        cfg = JSON.parse({
          cron: "0 0 1 1 *", "routing-key": "x", body: "y",
        }.to_json)
        param = LavinMQ::Parameter.new("scheduled-job", "via-param", cfg)
        vhost.add_parameter(param)
        vhost.scheduled_jobs.has_key?("via-param").should be_true
        vhost.delete_parameter("scheduled-job", "via-param")
        vhost.scheduled_jobs.has_key?("via-param").should be_false
      end
    end
  end

  describe "delete behavior" do
    it "delete is a no-op for an unknown name" do
      with_amqp_server do |s|
        s.vhosts["/"].scheduled_jobs.delete("never-existed").should be_nil
      end
    end

    it "create with an existing name terminates the previous runner" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        cfg = JSON.parse({
          cron: "0 0 1 1 *", "routing-key": "x", body: "y",
        }.to_json)
        first = vhost.scheduled_jobs.create("replace-me", cfg)
        second = vhost.scheduled_jobs.create("replace-me", cfg)
        first.state.should eq LavinMQ::ScheduledJob::Runner::State::Terminated
        second.state.should eq LavinMQ::ScheduledJob::Runner::State::Scheduled
        vhost.scheduled_jobs.delete("replace-me")
      end
    end
  end

  describe "stats persistence" do
    it "restores run_count from disk after store reload" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        cfg = JSON.parse({
          cron:          "0 0 1 1 *",
          exchange:      "",
          "routing-key": "noop",
          body:          "x",
        }.to_json)
        runner = vhost.scheduled_jobs.create("persist-job", cfg)
        runner.run_now
        wait_for { runner.run_count > 0 }
        runner.run_count.should eq 1
        vhost.scheduled_jobs["persist-job"].terminate
        vhost.scheduled_jobs.flush! # debounced writer needs an explicit sync

        # Build a fresh store on the same data dir; stats file should be loaded.
        fresh = LavinMQ::ScheduledJob::Store.new(vhost)
        restored = fresh.create("persist-job", cfg)
        restored.run_count.should eq 1
        fresh.close
        vhost.scheduled_jobs.delete("persist-job")
      end
    end
  end
end
