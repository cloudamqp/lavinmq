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
          uptime:       @server.uptime.total_milliseconds.to_i64,
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
          mem_limit:          @server.mem_limit,
          mem_used:           @server.rss,
          mem_used_details:   {log: @server.rss_log},
          io_write_count:     @server.blocks_out,
          io_write_details:   {log: @server.blocks_out_log},
          io_read_count:      @server.blocks_in,
          io_read_details:    {log: @server.blocks_in_log},
          cpu_user_time:      @server.user_time,
          cpu_user_details:   {log: @server.user_time_log},
          cpu_sys_time:       @server.sys_time,
          cpu_sys_details:    {log: @server.sys_time_log},
          db_dir:             @server.data_dir,
          disk_total:         @server.disk_total,
          disk_total_details: {log: @server.disk_total_log},
          disk_free:          @server.disk_free,
          disk_free_details:  {log: @server.disk_free_log},
          partitions:         Tuple.new,
          proc_used:          Fiber.count,
          run_queue:          0,
          sockets_used:       @server.vhosts.sum { |_, v| v.connections_size },
          followers:          @server.followers,
        }
      end

      private def gc_stats
        ps = GC.prof_stats
        {
          gc_no:                     ps.gc_no,
          heap_size:                 ps.heap_size,
          free_bytes:                ps.free_bytes,
          unmapped_bytes:            ps.unmapped_bytes,
          bytes_since_gc:            ps.bytes_since_gc,
          bytes_before_gc:           ps.bytes_before_gc,
          non_gc_bytes:              ps.non_gc_bytes,
          markers_m1:                ps.markers_m1,
          bytes_reclaimed_since_gc:  ps.bytes_reclaimed_since_gc,
          reclaimed_bytes_before_gc: ps.reclaimed_bytes_before_gc,
          expl_freed_bytes_since_gc: ps.expl_freed_bytes_since_gc,
          obtained_from_os_bytes:    ps.obtained_from_os_bytes,
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

        # Garbage collector profiling stats for the current node.
        # Registered before the :name route so it isn't matched as a node name.
        get "/api/nodes/gc_stats" do |context, _params|
          refuse_unless_administrator(context, user(context))
          gc_stats.to_json(context.response)
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

        # Run GC on the current node, without having to know its name
        post "/api/nodes/gc_collect" do |context, _params|
          refuse_unless_administrator(context, user(context))
          GC.collect
          context.response.status_code = 204
          context
        end

        post "/api/nodes/:name/gc_collect" do |context, params|
          refuse_unless_administrator(context, user(context))
          if params["name"] == System.hostname
            GC.collect
            context.response.status_code = 204
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
