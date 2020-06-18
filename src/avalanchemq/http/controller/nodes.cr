require "../controller"

module AvalancheMQ
  module HTTP
    class NodesController < Controller
      private def register_routes
        get "/api/nodes" do |context, _params|
          JSON.build(context.response) do |json|
            json.array do
              {
                uptime:              @amqp_server.uptime.total_milliseconds.to_i64,
                os_pid:              Process.pid,
                fd_total:            System.file_descriptor_limit[0],
                fd_used:             System.file_descriptor_count,
                processors:          System.cpu_count,
                mem_limit:           System.physical_memory,
                mem_used:            @amqp_server.max_rss,
                mem_used_details:    { log: @amqp_server.max_rss_log },
                io_write_count:      @amqp_server.blocks_out,
                io_write_details:    { log: @amqp_server.blocks_out_log },
                io_read_count:       @amqp_server.blocks_in,
                io_read_details:     { log: @amqp_server.blocks_in_log },
                cpu_user_time:       @amqp_server.user_time,
                cpu_user_details:    { log: @amqp_server.user_time_log },
                cpu_sys_time:        @amqp_server.sys_time,
                cpu_sys_details:     { log: @amqp_server.sys_time_log },
                db_dir:              @amqp_server.data_dir,
                name:                System.hostname,
                disk_total:          @amqp_server.disk_total,
                disk_total_details:  { log: @amqp_server.disk_total_log },
                disk_free:           @amqp_server.disk_free,
                disk_free_details:   { log: @amqp_server.disk_free_log },
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
