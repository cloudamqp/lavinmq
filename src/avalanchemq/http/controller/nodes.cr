require "../controller"
require "../../resource"

module AvalancheMQ
  module HTTP
    class NodesController < Controller
      private def register_routes
        get "/api/nodes" do |context, _params|
          gc_stats = GC.stats
          JSON.build(context.response) do |json|
            json.array do
              {
                uptime: @amqp_server.uptime,
                os_pid: Process.pid,
                fd_total: System.file_descriptor_limit,
                fd_used: 0,
                mem_limit: System.physical_memory,
                mem_used: gc_stats.total_bytes,
                processors: System.cpu_count,
                db_dir: @amqp_server.data_dir,
                name: System.hostname,
                connections_created: 0,
                connections_closed: 0,
                channels_created: 0,
                channels_closed: 0,
                queues_declared: 0,
                queues_deleted: 0,
              }.to_json(context.response)
            end
          end
          context
        end
      end
    end
  end
end
