require "../controller"

module AvalancheMQ
  module HTTP
    class NodesController < Controller
      private def register_routes
        get "/api/nodes" do |context, _params|
          fs_stats = Filesystem.info(@amqp_server.data_dir)
          rusage = System.resource_usage
          JSON.build(context.response) do |json|
            json.array do
              {
                uptime:              @amqp_server.uptime.total_milliseconds.to_i,
                os_pid:              Process.pid,
                fd_total:            System.file_descriptor_limit,
                fd_used:             System.file_descriptor_count,
                processors:          System.cpu_count,
                mem_limit:           System.physical_memory,
                mem_used:            rusage.max_rss,
                io_write_count:      rusage.blocks_out,
                io_read_count:       rusage.blocks_in,
                cpu_user_time:       rusage.user_time.total_milliseconds.to_i,
                cpu_sys_time:        rusage.sys_time.total_milliseconds.to_i,
                db_dir:              @amqp_server.data_dir,
                name:                System.hostname,
                disk_total:          fs_stats.total,
                disk_free:           fs_stats.available,
                connections_created: 0,
                connections_closed:  0,
                channels_created:    0,
                channels_closed:     0,
                queues_declared:     0,
                queues_deleted:      0,
                applications:        APPLICATIONS,
              }.to_json(context.response)
            end
          end
          context
        end
      end

      APPLICATIONS = [{
        name: "avalanchemq",
        description: "AvalancheMQ",
        version: AvalancheMQ::VERSION
      }]
    end
  end
end
