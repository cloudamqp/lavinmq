require "../controller.cr"
require "../../job_queue/job_tracker"

module LavinMQ
  module HTTP
    class JobQueuesController < Controller
      private def register_routes
        # Dashboard stats
        get "/api/job-queues/dashboard" do |context, _params|
          vhost = @amqp_server.vhosts["/"]
          tracker = vhost.job_tracker
          today = Time.utc.at_beginning_of_day

          stats = if tracker
                    {
                      "processed_today"  => tracker.metrics_aggregator.processed_count(today),
                      "failed_today"     => tracker.metrics_aggregator.failed_count(today),
                      "scheduled_count"  => tracker.scheduled_jobs_count,
                      "retry_count"      => tracker.retry_jobs_count,
                      "dead_count"       => tracker.dead_jobs_count,
                      "active_processes" => tracker.process_tracker.process_count,
                      "busy_workers"     => tracker.process_tracker.total_busy_workers,
                      "total_workers"    => tracker.process_tracker.total_workers,
                      "queue_count"      => 0,
                    }
                  else
                    {
                      "processed_today"  => 0,
                      "failed_today"     => 0,
                      "scheduled_count"  => 0,
                      "retry_count"      => 0,
                      "dead_count"       => 0,
                      "active_processes" => 0,
                      "busy_workers"     => 0,
                      "total_workers"    => 0,
                      "queue_count"      => 0,
                    }
                  end

          context.response.content_type = "application/json"
          stats.to_json(context.response)
          context
        end

        # Scheduled jobs
        get "/api/job-queues/scheduled-jobs" do |context, _params|
          vhost = @amqp_server.vhosts["/"]
          tracker = vhost.job_tracker

          jobs = tracker ? tracker.scheduled_jobs(100) : [] of JobQueue::ScheduledJob

          context.response.content_type = "application/json"
          jobs.to_json(context.response)
          context
        end

        # Retry jobs
        get "/api/job-queues/retries" do |context, _params|
          vhost = @amqp_server.vhosts["/"]
          tracker = vhost.job_tracker

          jobs = tracker ? tracker.retry_jobs(100) : [] of JobQueue::RetryJob

          context.response.content_type = "application/json"
          jobs.to_json(context.response)
          context
        end

        # Dead jobs
        get "/api/job-queues/dead-jobs" do |context, _params|
          vhost = @amqp_server.vhosts["/"]
          tracker = vhost.job_tracker

          jobs = tracker ? tracker.dead_jobs(100) : [] of JobQueue::DeadJob

          context.response.content_type = "application/json"
          jobs.to_json(context.response)
          context
        end

        # Worker processes
        get "/api/job-queues/processes" do |context, _params|
          vhost = @amqp_server.vhosts["/"]
          tracker = vhost.job_tracker

          processes = tracker ? tracker.process_tracker.active_processes : [] of JobQueue::Heartbeat

          context.response.content_type = "application/json"
          processes.to_json(context.response)
          context
        end

        # Metrics
        get "/api/job-queues/metrics" do |context, _params|
          vhost = @amqp_server.vhosts["/"]
          tracker = vhost.job_tracker

          if tracker
            top_jobs = tracker.metrics_aggregator.top_jobs(24.hours, 100)
            metrics = top_jobs.map do |(job_class, bucket)|
              {
                "job_class" => job_class,
                "count"     => bucket.count,
                "success"   => bucket.success,
                "failed"    => bucket.failed,
                "total_ms"  => bucket.total_ms,
              }
            end
          else
            metrics = [] of Hash(String, Int64 | String)
          end

          context.response.content_type = "application/json"
          metrics.to_json(context.response)
          context
        end
      end
    end
  end
end
