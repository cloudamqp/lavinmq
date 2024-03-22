require "../controller"

module LavinMQ
  module HTTP
    class NodesController < Controller
      include StatsHelpers

      SERVER_METRICS = {:connection_created, :connection_closed, :channel_created, :channel_closed,
                        :queue_declared, :queue_deleted}

      private def vhost_stats(vhosts)
        messages_unacknowledged = 0_u64
        messages_ready = 0_u64

        {% for sm in SERVER_METRICS %}
          {{sm.id}} = 0_u64
          {{sm.id}}_rate = 0_f64
          {{sm.id}}_log = Deque(Float64).new(LavinMQ::Config.instance.stats_log_size)
        {% end %}
        vhosts.each do |vhost|
          messages_unacknowledged += vhost.message_details[:messages_unacknowledged]
          messages_ready += vhost.message_details[:messages_ready]
          {% for sm in SERVER_METRICS %}
            {{sm.id}} += vhost.stats_details[:{{sm.id}}]
            {{sm.id}}_rate += vhost.stats_details[:{{sm.id}}_details][:rate]
            add_logs!({{sm.id}}_log, vhost.stats_details[:{{sm.id}}_details][:log])
          {% end %}
        end
        {% begin %}
        {
          messages_unacknowledged: messages_unacknowledged,
          messages_ready: messages_ready,
          {% for sm in SERVER_METRICS %}
            {{sm.id}}: {{sm.id}},
            {{sm.id}}_details: {
              rate: {{sm.id}}_rate,
              log: {{sm.id}}_log,
            },
          {% end %}
        }
        {% end %}
      end

      private def general_stats
        {
          uptime:       @amqp_server.uptime.total_milliseconds.to_i64,
          running:      true,
          name:         @amqp_server.cluster_name,
          applications: APPLICATIONS,
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
          partitions:         Tuple.new,
          proc_used:          Fiber.count,
          run_queue:          0,
          sockets_used:       @amqp_server.vhosts.sum { |_, v| v.connections.size },
          followers:          @amqp_server.followers,
        }
      end

      private def stats(context)
        current_user = user(context)
        my_vhosts = vhosts(current_user)
        selected = context.request.query_params.fetch_all("vhost")
        my_vhosts = my_vhosts.select { |vhost| selected.includes? vhost.name } unless selected.empty?
        if current_user.tags.any?(&.administrator?)
          general_stats + node_stats + vhost_stats(my_vhosts)
        else
          general_stats + vhost_stats(my_vhosts)
        end
      end

      private def register_routes
        get "/api/nodes" do |context, _params|
          Tuple.new(stats(context)).to_json(context.response)
          context
        end

        get "/api/nodes/:name" do |context, params|
          if params["name"] == System.hostname
            stats(context).to_json(context.response)
          else
            context.response.status_code = 404
          end
          context
        end
      end

      APPLICATIONS = [{
        name:        "lavinmq",
        description: "LavinMQ",
        version:     LavinMQ::VERSION,
      }]
    end
  end
end
