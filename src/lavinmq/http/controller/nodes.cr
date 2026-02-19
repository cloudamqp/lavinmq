require "../controller"
require "../stats_helper"

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
          {{ sm.id }} = 0_u64
          {{ sm.id }}_rate = 0_f64
          {{ sm.id }}_log = Deque(Float64).new(LavinMQ::Config.instance.stats_log_size)
        {% end %}
        vhosts.each do |vhost|
          message_details = vhost.message_details
          messages_unacknowledged += message_details[:messages_unacknowledged]
          messages_ready += message_details[:messages_ready]
          stats_details = vhost.stats_details
          {% for sm in SERVER_METRICS %}
            {{ sm.id }} += stats_details[:{{ sm.id }}]
            {{ sm.id }}_rate += stats_details[:{{ sm.id }}_details][:rate]
            add_logs!({{ sm.id }}_log, stats_details[:{{ sm.id }}_details][:log])
          {% end %}
        end
        {% begin %}
        {
          messages_unacknowledged: messages_unacknowledged,
          messages_ready: messages_ready,
          {% for sm in SERVER_METRICS %}
            {{ sm.id }}: {{ sm.id }},
            {{ sm.id }}_details: {
              rate: {{ sm.id }}_rate,
              log: {{ sm.id }}_log,
            },
          {% end %}
        }
        {% end %}
      end

      private def general_stats
        {
          uptime:       @amqp_server.uptime.total_milliseconds.to_i64,
          running:      true,
          name:         System.hostname,
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
          merged_replicas:    merged_replicas,
        }
      end

      # Merges all known replicas from etcd with connected followers
      private def merged_replicas
        known = @amqp_server.known_replicas
        connected = @amqp_server.all_followers
        connected_by_id = connected.to_h { |f| {f.id.to_s(36), f} }
        leader_id = @amqp_server.leader_id.try(&.to_s(36))

        # Build list from all known replicas
        result = known.map do |id, insync|
          is_leader = id == leader_id
          if follower = connected_by_id[id]?
            {
              id:             id,
              role:           is_leader ? "leader" : "follower",
              insync:         insync,
              remote_address: follower.remote_address.to_s,
              sent_bytes:     follower.sent_bytes,
              acked_bytes:    follower.acked_bytes,
            }
          else
            {
              id:             id,
              role:           is_leader ? "leader" : "follower",
              insync:         insync,
              remote_address: nil,
              sent_bytes:     nil,
              acked_bytes:    nil,
            }
          end
        end

        # Sort with leader first
        result.sort_by! { |r| r[:role] == "leader" ? 0 : 1 }
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

        delete "/api/nodes/:id" do |context, params|
          refuse_unless_administrator(context, user(context))
          id_str = params["id"]
          id = id_str.to_i32?(36)
          unless id
            bad_request(context, "Invalid replica ID")
          end
          # Don't allow forgetting the leader
          if @amqp_server.leader_id == id
            bad_request(context, "Cannot forget the leader node")
          end
          # Don't allow forgetting connected replicas
          if @amqp_server.all_followers.any? { |f| f.id == id }
            bad_request(context, "Cannot forget a connected replica")
          end
          if @amqp_server.forget_replica(id)
            context.response.status_code = 204
          else
            not_found(context, "Replica not found")
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
