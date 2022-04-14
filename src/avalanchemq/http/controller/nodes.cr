require "../controller"

module AvalancheMQ
  module HTTP
    class NodesController < Controller
      include StatsHelpers

      # channel_closed channel_created connection_closed connection_created queue_declared queue_deleted
      SERVER_METRICS = {:connection_created, :connection_closed, :channel_created, :channel_closed,
                        :queue_declared, :queue_deleted}

      private def vhost_stats
        {% for sm in SERVER_METRICS %}
          {{sm.id}} = 0_u64
          {{sm.id}}_rate = 0_f64
          {{sm.id}}_log = Deque(Float64).new
        {% end %}

        @amqp_server.vhosts.each_value do |vhost|
          {% for sm in SERVER_METRICS %}
            # connection_created += vhost.stats_details[:connection_created]
            {{sm.id}} += vhost.stats_details[:{{sm.id}}]
            {{sm.id}}_rate += vhost.stats_details[:{{sm.id}}_details][:rate]
            add_logs!({{sm.id}}_log, vhost.stats_details[:{{sm.id}}_details][:log])
          {% end %}
        end

        # NamedTuple.new(
        #   {% for sm in SERVER_METRICS %}
        #   {{sm.id}}: {{sm}},
        #   {{sm.id}}_details: {
        #     rate: {{sm.id}}_rate,
        #     log: {{sm.id}}_log
        #   }
        #   {% end %}
        # )
        NamedTuple.new

        # connection_created_details: {
        #   rate: connection_created_rate,
        # log: connection_created_log,
        # },
        # connection_closed:         @amqp_server.connection_closed_count,
        # connection_closed_details: {
        #   rate: @amqp_server.connection_closed_rate,
        #   # log:  @amqp_server.connection_closed_log,
        # },
        # channel_created:         @amqp_server.channel_created_count,
        # channel_created_details: {
        #   rate: @amqp_server.channel_created_rate,
        #   # log:  @amqp_server.channel_created_log,
        # },
        # channel_closed:         @amqp_server.channel_closed_count,
        # channel_closed_details: {
        #   rate: @amqp_server.channel_closed_rate,
        #   # log:  @amqp_server.channel_closed_log,
        # },
        # queue_declared:         @amqp_server.queue_declared_count,
        # queue_declared_details: {
        #   rate: @amqp_server.queue_declared_rate,
        #   # log:  @amqp_server.queue_declared_log,
        # },
        # queue_deleted:         @amqp_server.queue_deleted_count,
        # queue_deleted_details: {
        #   rate: @amqp_server.queue_deleted_rate,
        #   # log:  @amqp_server.queue_deleted_log,
        # }

      end

      private def general_stats
        {
          uptime:  @amqp_server.uptime.total_milliseconds.to_i64,
          running: true,
          name:    System.hostname,

        }
      end

      private def node_stats
        {
          os_pid:             Process.pid.to_s,
          fd_total:           System.file_descriptor_limit[0],
          fd_used:            System.file_descriptor_count,
          processors:         System.cpu_count,
          mem_limit:          @amqp_server.mem_limit,
          mem_used:           @amqp_server.rss,
          mem_used_details:   {log: @amqp_server.rss_log},
          io_write_count:     @amqp_server.blocks_out,
          io_write_details:   {log: @amqp_server.blocks_out_log},
          io_read_count:      @amqp_server.blocks_in,
          io_read_details:    {log: @amqp_server.blocks_in_log},
          cpu_user_time:      @amqp_server.user_time,
          cpu_user_details:   {log: @amqp_server.user_time_log},
          cpu_sys_time:       @amqp_server.sys_time,
          cpu_sys_details:    {log: @amqp_server.sys_time_log},
          db_dir:             @amqp_server.data_dir,
          disk_total:         @amqp_server.disk_total,
          disk_total_details: {log: @amqp_server.disk_total_log},
          disk_free:          @amqp_server.disk_free,
          disk_free_details:  {log: @amqp_server.disk_free_log},
          applications:       APPLICATIONS,
          partitions:         Tuple.new,
          proc_used:          Fiber.count,
          run_queue:          0,
          sockets_used:       @amqp_server.vhosts.sum { |_, v| v.connections.size },
        }
      end

      private def register_routes
        get "/api/nodes" do |context, _params|
          stats = general_stats + node_stats + vhost_stats
          Tuple.new(stats).to_json(context.response)
          context
        end

        get "/api/nodes/:name" do |context, params|
          if params[:name] == System.hostname
            stats = general_stats + node_stats + vhost_stats
            stats.to_json(context.response)
          else
            context.response.status_code = 404
          end
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
