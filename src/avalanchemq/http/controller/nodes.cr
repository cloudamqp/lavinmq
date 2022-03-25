require "../controller"

module AvalancheMQ
  module HTTP
    class NodesController < Controller
      private def nodes_info
        Tuple.new({
          uptime:                     @amqp_server.uptime.total_milliseconds.to_i64,
          running:                    true,
          os_pid:                     Process.pid.to_s,
          fd_total:                   System.file_descriptor_limit[0],
          fd_used:                    System.file_descriptor_count,
          processors:                 System.cpu_count,
          mem_limit:                  @amqp_server.mem_limit,
          mem_used:                   @amqp_server.rss,
          mem_used_details:           {log: @amqp_server.rss_log},
          io_write_count:             @amqp_server.blocks_out,
          io_write_details:           {log: @amqp_server.blocks_out_log},
          io_read_count:              @amqp_server.blocks_in,
          io_read_details:            {log: @amqp_server.blocks_in_log},
          cpu_user_time:              @amqp_server.user_time,
          cpu_user_details:           {log: @amqp_server.user_time_log},
          cpu_sys_time:               @amqp_server.sys_time,
          cpu_sys_details:            {log: @amqp_server.sys_time_log},
          db_dir:                     @amqp_server.data_dir,
          name:                       System.hostname,
          disk_total:                 @amqp_server.disk_total,
          disk_total_details:         {log: @amqp_server.disk_total_log},
          disk_free:                  @amqp_server.disk_free,
          disk_free_details:          {log: @amqp_server.disk_free_log},
          connection_created:         @amqp_server.connection_created_count,
          connection_created_details: {rate: @amqp_server.connection_created_rate,
                                       log: @amqp_server.connection_created_log},
          connection_closed:         @amqp_server.connection_closed_count,
          connection_closed_details: {rate: @amqp_server.connection_closed_rate,
                                      log: @amqp_server.connection_closed_log},
          channel_created:         @amqp_server.channel_created_count,
          channel_created_details: {rate: @amqp_server.channel_created_rate,
                                    log: @amqp_server.channel_created_log},
          channel_closed:         @amqp_server.channel_closed_count,
          channel_closed_details: {rate: @amqp_server.channel_closed_rate,
                                   log: @amqp_server.channel_closed_log},
          queue_declared:         @amqp_server.queue_declared_count,
          queue_declared_details: {rate: @amqp_server.queue_declared_rate,
                                   log: @amqp_server.queue_declared_log},
          queue_deleted:         @amqp_server.queue_deleted_count,
          queue_deleted_details: {rate: @amqp_server.queue_deleted_rate,
                                  log: @amqp_server.queue_deleted_log},
          applications: APPLICATIONS,
          partitions:   Tuple.new,
          proc_used:    0,
          run_queue:    0,
          sockets_used: @amqp_server.vhosts.sum { |_, v| v.connections.size },
        })
      end

      private def register_routes
        get "/api/nodes" do |context, _params|
          nodes_info.to_json(context.response)
          context
        end

        get "/api/nodes/:name" do |context, params|
          node = nodes_info.find { |n| n[:name] == params["name"] }
          context.response.status_code = 404 unless node
          node.to_json(context.response) if node
          context
        end
      end

      APPLICATIONS = [{
        name:        "avalanchemq",
        description: "AvalancheMQ",
        version:     AvalancheMQ::VERSION,
      }]
    end
  end
end
