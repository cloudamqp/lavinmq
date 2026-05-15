require "../controller.cr"

module LavinMQ
  module HTTP
    class ScheduledJobsController < Controller
      private def register_routes
        get "/api/scheduled-jobs" do |context, _params|
          arr = vhosts(user(context)).flat_map do |v|
            v.scheduled_jobs.values
          end
          page(context, arr)
        end

        get "/api/scheduled-jobs/:vhost" do |context, params|
          with_vhost(context, params) do |vhost|
            page(context, vhost.scheduled_jobs.values)
          end
        end

        get "/api/scheduled-jobs/:vhost/:name" do |context, params|
          with_vhost(context, params) do |vhost|
            if job = vhost.scheduled_jobs[params["name"]]?
              job.to_json(context.response)
            else
              not_found(context)
            end
          end
        end

        put "/api/scheduled-jobs/:vhost/:name/pause" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            if job = vhost.scheduled_jobs[params["name"]]?
              if job.pause
                context.response.status_code = 204
              else
                context.response.status_code = 422
              end
            else
              not_found(context)
            end
          end
        end

        put "/api/scheduled-jobs/:vhost/:name/resume" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            if job = vhost.scheduled_jobs[params["name"]]?
              if job.resume
                context.response.status_code = 204
              else
                context.response.status_code = 422
              end
            else
              not_found(context)
            end
          end
        end

        post "/api/scheduled-jobs/:vhost/:name/run" do |context, params|
          with_vhost(context, params) do |vhost|
            refuse_unless_policymaker(context, user(context), vhost)
            if job = vhost.scheduled_jobs[params["name"]]?
              if job.run_now
                context.response.status_code = 202
              else
                context.response.status_code = 422
              end
            else
              not_found(context)
            end
          end
        end
      end
    end
  end
end
