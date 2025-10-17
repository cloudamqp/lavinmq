require "../controller.cr"

module LavinMQ
  module HTTP
    class ScheduledJobsController < Controller
      include StatsHelpers

      private def register_routes
        get "/api/scheduled-jobs" do |context, _params|
          itrs = vhosts(user(context)).compact_map do |v|
            v.scheduled_jobs.try(&.each_value)
          end.flatten
          page(context, itrs)
        end

        get "/api/scheduled-jobs/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            if jobs = @amqp_server.vhosts[vhost].scheduled_jobs
              page(context, jobs.each_value)
            else
              context.response.status_code = 200
              context.response.content_type = "application/json"
              context.response.print "[]"
            end
          end
        end

        get "/api/scheduled-jobs/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            job_name = params["name"]
            if jobs = @amqp_server.vhosts[vhost].scheduled_jobs
              if job = jobs[job_name]?
                job.to_json(context.response)
              else
                context.response.status_code = 404
              end
            else
              context.response.status_code = 404
            end
          end
        end

        put "/api/scheduled-jobs/:vhost/:name/enable" do |context, params|
          with_vhost(context, params) do |vhost|
            job_name = params["name"]
            if jobs = @amqp_server.vhosts[vhost].scheduled_jobs
              if job = jobs[job_name]?
                job.enable
                context.response.status_code = 204
              else
                context.response.status_code = 404
              end
            else
              context.response.status_code = 404
            end
          end
        end

        put "/api/scheduled-jobs/:vhost/:name/disable" do |context, params|
          with_vhost(context, params) do |vhost|
            job_name = params["name"]
            if jobs = @amqp_server.vhosts[vhost].scheduled_jobs
              if job = jobs[job_name]?
                job.disable
                context.response.status_code = 204
              else
                context.response.status_code = 404
              end
            else
              context.response.status_code = 404
            end
          end
        end

        put "/api/scheduled-jobs/:vhost/:name/trigger" do |context, params|
          with_vhost(context, params) do |vhost|
            job_name = params["name"]
            if jobs = @amqp_server.vhosts[vhost].scheduled_jobs
              if job = jobs[job_name]?
                job.trigger
                context.response.status_code = 204
              else
                context.response.status_code = 404
              end
            else
              context.response.status_code = 404
            end
          end
        end
      end
    end
  end
end
