require "./spec_helper"

describe LavinMQ::JobQueue do
  describe "Stream queues" do
    it "should create job queue streams on vhost initialization" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.queues["scheduled_jobs"]?.should_not be_nil
        vhost.queues["retries"]?.should_not be_nil
        vhost.queues["dead_jobs"]?.should_not be_nil
        vhost.queues["heartbeats"]?.should_not be_nil
        vhost.queues["job_metrics"]?.should_not be_nil
      end
    end

    it "should create stream-type queues" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        scheduled = vhost.queues["scheduled_jobs"]?
        scheduled.should be_a(LavinMQ::AMQP::Stream)
      end
    end
  end

  describe "JobTracker" do
    it "should initialize job tracker on vhost creation" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.job_tracker.should_not be_nil
        vhost.job_tracker.should be_a(LavinMQ::JobQueue::JobTracker)
      end
    end

    it "should start scheduler fibers" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        tracker = vhost.job_tracker.not_nil!
        tracker.scheduled_job_scheduler.should_not be_nil
        tracker.retry_scheduler.should_not be_nil
        tracker.process_tracker.should_not be_nil
        tracker.metrics_aggregator.should_not be_nil
      end
    end

    it "should return empty arrays for jobs initially" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        tracker = vhost.job_tracker.not_nil!
        tracker.scheduled_jobs.should eq([] of LavinMQ::JobQueue::ScheduledJob)
        tracker.retry_jobs.should eq([] of LavinMQ::JobQueue::RetryJob)
        tracker.dead_jobs.should eq([] of LavinMQ::JobQueue::DeadJob)
      end
    end

    it "should return zero counts initially" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        tracker = vhost.job_tracker.not_nil!
        tracker.scheduled_jobs_count.should eq(0)
        tracker.retry_jobs_count.should eq(0)
        tracker.dead_jobs_count.should eq(0)
      end
    end
  end

  describe "ProcessTracker" do
    it "should track active processes" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        tracker = vhost.job_tracker.not_nil!.process_tracker
        tracker.active_processes.should be_empty
        tracker.process_count.should eq(0)
        tracker.total_workers.should eq(0)
        tracker.total_busy_workers.should eq(0)
      end
    end
  end

  describe "MetricsAggregator" do
    it "should return zero counts initially" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        aggregator = vhost.job_tracker.not_nil!.metrics_aggregator
        aggregator.processed_count(Time.utc.at_beginning_of_day).should eq(0)
        aggregator.failed_count(Time.utc.at_beginning_of_day).should eq(0)
      end
    end

    it "should return nil for job stats when no data" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        aggregator = vhost.job_tracker.not_nil!.metrics_aggregator
        aggregator.stats_for_job("NonExistentJob").should be_nil
      end
    end

    it "should return overall stats" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        aggregator = vhost.job_tracker.not_nil!.metrics_aggregator
        stats = aggregator.overall_stats(24.hours)
        stats.count.should eq(0)
        stats.total_ms.should eq(0)
        stats.success.should eq(0)
        stats.failed.should eq(0)
      end
    end
  end

  describe "HTTP API" do
    it "should respond to dashboard endpoint" do
      with_http_server do |http, s|
        response = http.get("/api/job-queues/dashboard")
        response.status_code.should eq(200)
        response.headers["Content-Type"].should contain("application/json")
      end
    end

    it "should respond to scheduled jobs endpoint" do
      with_http_server do |http, s|
        response = http.get("/api/job-queues/scheduled-jobs")
        response.status_code.should eq(200)
        response.body.should eq("[]")
      end
    end

    it "should respond to retries endpoint" do
      with_http_server do |http, s|
        response = http.get("/api/job-queues/retries")
        response.status_code.should eq(200)
        response.body.should eq("[]")
      end
    end

    it "should respond to dead jobs endpoint" do
      with_http_server do |http, s|
        response = http.get("/api/job-queues/dead-jobs")
        response.status_code.should eq(200)
        response.body.should eq("[]")
      end
    end

    it "should respond to processes endpoint" do
      with_http_server do |http, s|
        response = http.get("/api/job-queues/processes")
        response.status_code.should eq(200)
        response.body.should eq("[]")
      end
    end

    it "should respond to metrics endpoint" do
      with_http_server do |http, s|
        response = http.get("/api/job-queues/metrics")
        response.status_code.should eq(200)
        response.body.should eq("[]")
      end
    end
  end
end
