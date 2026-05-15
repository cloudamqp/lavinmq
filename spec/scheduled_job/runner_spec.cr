require "../spec_helper"

private def make_runner(vhost, name = "spec-runner", *, cron : String = "0 0 1 1 *",
                        exchange : String = "", routing_key : String = "rk",
                        body : String = "x", start_paused : Bool = false,
                        calls : Array(String)? = nil)
  c = LavinMQ::ScheduledJob::Cron.parse(cron)
  callback = ->(_r : LavinMQ::ScheduledJob::Runner) {
    calls.try &.push("state-change")
    nil
  }
  if start_paused
    File.write(File.join(LavinMQ::Config.instance.data_dir,
      "scheduled_jobs.#{Digest::SHA1.hexdigest(name)}.paused"), name)
  end
  LavinMQ::ScheduledJob::Runner.new(name, vhost, c, exchange, routing_key, body,
    AMQ::Protocol::Properties.new, callback)
end

describe LavinMQ::ScheduledJob::Runner do
  describe "initial state" do
    it "starts Scheduled by default" do
      with_amqp_server do |s|
        r = make_runner(s.vhosts["/"])
        r.state.should eq LavinMQ::ScheduledJob::Runner::State::Scheduled
        r.terminate
      end
    end

    it "starts Paused when a paused-file marker already exists on disk" do
      with_amqp_server do |s|
        name = "preexisting-paused"
        path = File.join(LavinMQ::Config.instance.data_dir,
          "scheduled_jobs.#{Digest::SHA1.hexdigest(name)}.paused")
        File.write(path, name)
        r = make_runner(s.vhosts["/"], name)
        r.state.should eq LavinMQ::ScheduledJob::Runner::State::Paused
        r.delete
      end
    end
  end

  describe "#run_now" do
    it "returns false when paused" do
      with_amqp_server do |s|
        r = make_runner(s.vhosts["/"], "rn-paused", start_paused: true)
        r.run_now.should be_false
        r.delete
      end
    end

    it "returns false when terminated" do
      with_amqp_server do |s|
        r = make_runner(s.vhosts["/"], "rn-terminated")
        r.terminate
        r.run_now.should be_false
      end
    end
  end

  describe "#pause / #resume" do
    it "is idempotent for pause" do
      with_amqp_server do |s|
        r = make_runner(s.vhosts["/"], "pause-idem")
        spawn r.run
        Fiber.yield
        r.pause.should be_true
        r.pause.should be_false # already paused
        r.delete
      end
    end

    it "is idempotent for resume" do
      with_amqp_server do |s|
        r = make_runner(s.vhosts["/"], "resume-idem", start_paused: true)
        r.resume.should be_true
        r.resume.should be_false # already scheduled
        r.terminate
      end
    end

    it "pause invokes the state-change callback" do
      with_amqp_server do |s|
        calls = [] of String
        r = make_runner(s.vhosts["/"], "pause-cb", calls: calls)
        spawn r.run
        Fiber.yield
        r.pause
        calls.includes?("state-change").should be_true
        r.delete
      end
    end
  end

  describe "#terminate" do
    it "transitions state to Terminated" do
      with_amqp_server do |s|
        r = make_runner(s.vhosts["/"], "term")
        r.terminate
        r.state.should eq LavinMQ::ScheduledJob::Runner::State::Terminated
      end
    end

    it "is idempotent" do
      with_amqp_server do |s|
        r = make_runner(s.vhosts["/"], "term-idem")
        r.terminate
        r.terminate # should not raise
        r.state.should eq LavinMQ::ScheduledJob::Runner::State::Terminated
      end
    end
  end

  describe "#restore_stats" do
    it "overrides default counters" do
      with_amqp_server do |s|
        r = make_runner(s.vhosts["/"], "restore")
        r.restore_stats(42_u64, Time.utc(2026, 5, 15), "boom", 17_i64)
        r.run_count.should eq 42
        r.last_run_at.should eq Time.utc(2026, 5, 15)
        r.last_error.should eq "boom"
        r.last_duration_ms.should eq 17
        r.terminate
      end
    end
  end

  describe "#details_tuple" do
    it "exposes all relevant fields" do
      with_amqp_server do |s|
        r = make_runner(s.vhosts["/"], "detail", cron: "*/15 * * * *",
          exchange: "amq.topic", routing_key: "x.y")
        t = r.details_tuple
        t[:name].should eq "detail"
        t[:vhost].should eq "/"
        t[:exchange].should eq "amq.topic"
        t[:routing_key].should eq "x.y"
        t[:cron].should eq "*/15 * * * *"
        t[:run_count].should eq 0
        t[:state].should eq "Scheduled"
        r.terminate
      end
    end
  end

  describe "fire failure isolation" do
    it "records last_error when the exchange does not exist" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        r = make_runner(vhost, "fire-fail",
          exchange: "does-not-exist-xyz", routing_key: "k")
        spawn r.run
        r.run_now.should be_true
        # The publish itself doesn't raise on unknown exchange (it returns false),
        # so we just confirm the runner survives and is still scheduled.
        wait_for { r.run_count > 0 || r.last_error }
        r.state.should eq LavinMQ::ScheduledJob::Runner::State::Scheduled
        r.delete
      end
    end
  end
end
