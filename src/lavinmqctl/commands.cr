require "json"
require "uri"
require "../lavinmq/auth/user"
require "./definitions_generator"

module LavinMQCtl
  class Commands
    def initialize(@client : Client, @parser : Parser)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def run
      case @parser.cmd
      when "create_queue"          then create_queue
      when "delete_queue"          then delete_queue
      when "import_definitions"    then import_definitions
      when "export_definitions"    then export_definitions
      when "set_user_tags"         then set_user_tags
      when "add_user"              then add_user
      when "list_users"            then list_users
      when "delete_user"           then delete_user
      when "change_password"       then change_password
      when "list_queues"           then list_queues
      when "purge_queue"           then purge_queue
      when "pause_queue"           then pause_queue
      when "resume_queue"          then resume_queue
      when "list_vhosts"           then list_vhosts
      when "add_vhost"             then add_vhost
      when "delete_vhost"          then delete_vhost
      when "clear_policy"          then clear_policy
      when "list_policies"         then list_policies
      when "set_policy"            then set_policy
      when "list_connections"      then list_connections
      when "close_connection"      then close_connection
      when "close_all_connections" then close_all_connections
      when "list_exchanges"        then list_exchanges
      when "create_exchange"       then create_exchange
      when "delete_exchange"       then delete_exchange
      when "status"                then status
      when "cluster_status"        then cluster_status
      when "set_vhost_limits"      then set_vhost_limits
      when "set_permissions"       then set_permissions
      when "definitions"           then definitions
      when "hash_password"         then hash_password
      when "stop_app"
      when "start_app"
      else
        @parser.help
      end
    rescue ex : IO::Error
      abort ex
    end

    private def import_definitions
      file = ARGV.shift? || ""
      resp = if file == "-"
               @client.http.post "/api/definitions", @client.headers, STDIN
             elsif File.exists?(file)
               File.open(file) do |io|
                 @client.http.post "/api/definitions", @client.headers, io
               end
             else
               STDERR.puts "ERROR: File not found"
               abort @parser.banner
             end
      @client.handle_response(resp, 200)
    end

    private def export_definitions
      url = "/api/definitions"
      url += "/#{URI.encode_www_form(@client.options["vhost"])}" if @client.options.has_key?("vhost")
      resp = @client.http.get url, @client.headers
      @client.handle_response(resp, 200)
      @client.output resp.body
    end

    private def list_users
      puts "Listing users ..." unless @client.quiet?
      uu = @client.get("/api/users").map do |u|
        next unless user = u.as_h?
        {name: user["name"].to_s, tags: user["tags"].to_s}
      end
      @client.output uu
    end

    private def add_user
      username = ARGV.shift?
      password = ARGV.shift?
      abort @parser.banner unless username && password
      resp = @client.http.put "/api/users/#{username}", @client.headers, {password: password}.to_json
      @client.handle_response(resp, 201, 204)
    end

    private def delete_user
      username = ARGV.shift?
      abort @parser.banner unless username
      resp = @client.http.delete "/api/users/#{username}", @client.headers
      @client.handle_response(resp, 204)
    end

    private def set_user_tags
      username = ARGV.shift?
      tags = ARGV.join(",")
      abort @parser.banner unless username && tags
      resp = @client.http.put "/api/users/#{username}", @client.headers, {tags: tags}.to_json
      @client.handle_response(resp, 201, 204)
    end

    private def change_password
      username = ARGV.shift?
      pwd = ARGV.shift?
      abort @parser.banner unless username && pwd
      resp = @client.http.put "/api/users/#{username}", @client.headers, {password: pwd}.to_json
      @client.handle_response(resp, 204)
    end

    private def list_queues
      vhost = @client.options["vhost"]? || "/"
      puts "Listing queues for vhost #{vhost} ..." unless @client.quiet?
      qq = @client.get("/api/queues/#{URI.encode_www_form(vhost)}").map do |u|
        next unless q = u.as_h?
        {name: q["name"].to_s, messages: q["messages"].to_s}
      end
      @client.output qq
    end

    private def purge_queue
      vhost = @client.options["vhost"]? || "/"
      queue = ARGV.shift?
      abort @parser.banner unless queue
      resp = @client.http.delete "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/contents", @client.headers
      @client.handle_response(resp, 204)
    end

    private def pause_queue
      vhost = @client.options["vhost"]? || "/"
      queue = ARGV.shift?
      abort @parser.banner unless queue
      resp = @client.http.put "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/pause", @client.headers
      @client.handle_response(resp, 204)
    end

    private def resume_queue
      vhost = @client.options["vhost"]? || "/"
      queue = ARGV.shift?
      abort @parser.banner unless queue
      resp = @client.http.put "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/resume", @client.headers
      @client.handle_response(resp, 204)
    end

    private def list_connections
      columns = ARGV
      columns = ["user", "peer_host", "peer_port", "state"] if columns.empty?
      conns = @client.get("/api/connections")
      puts "Listing connections ..." unless @client.quiet?

      if @client.options["format"]? == "json"
        cc = conns.map do |u|
          next unless conn = u.as_h?
          conn.select { |k, _v| columns.includes? k }
        end
        @client.output cc
      else
        puts columns.join(STDOUT, "\t")
        conns.each do |u|
          if conn = u.as_h?
            columns.each_with_index do |c, i|
              case c
              when "client_properties"
                @client.print_erlang_terms(conn[c].as_h)
              else
                print conn[c]?
              end
              print "\t" unless i == columns.size - 1
            end
            puts
          end
        end
      end
    end

    private def close_connection
      name = ARGV.shift?
      abort @parser.banner unless name
      puts "Closing connection #{name} ..." unless @client.quiet?
      @client.headers["X-Reason"] = ARGV.shift? || "Closed via lavinmqctl"
      resp = @client.http.delete "/api/connections/#{URI.encode_path(name)}", @client.headers
      @client.handle_response(resp, 204)
    end

    private def close_all_connections
      conns = @client.get("/api/connections")
      closed_conns = [] of NamedTuple(name: String)
      @client.headers["X-Reason"] = ARGV.shift? || "Closed via lavinmqctl"
      conns.each do |u|
        next unless conn = u.as_h?
        name = conn["name"].to_s
        puts "Closing connection #{name} ..." unless @client.quiet?
        @client.http.delete "/api/connections/#{URI.encode_path(name)}", @client.headers
        closed_conns << {name: name}
      end
      @client.output closed_conns, ["closed_connections"]
    end

    private def list_vhosts
      puts "Listing vhosts ..." unless @client.quiet?
      vv = @client.get("/api/vhosts").map do |u|
        next unless v = u.as_h?
        {name: v["name"].to_s}
      end
      @client.output vv
    end

    private def add_vhost
      name = ARGV.shift?
      abort @parser.banner unless name
      resp = @client.http.put "/api/vhosts/#{URI.encode_www_form(name)}", @client.headers
      @client.handle_response(resp, 201, 204)
    end

    private def delete_vhost
      name = ARGV.shift?
      abort @parser.banner unless name
      resp = @client.http.delete "/api/vhosts/#{URI.encode_www_form(name)}", @client.headers
      @client.handle_response(resp, 204)
    end

    private def clear_policy
      vhost = @client.options["vhost"]? || "/"
      name = ARGV.shift?
      abort @parser.banner unless name
      resp = @client.http.delete "/api/policies/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}", @client.headers
      @client.handle_response(resp, 204)
    end

    private def list_policies
      vhost = @client.options["vhost"]? || "/"
      puts "Listing policies for vhost #{vhost} ..." unless @client.quiet?
      @client.output @client.get("/api/policies/#{URI.encode_www_form(vhost)}")
    end

    private def set_policy
      vhost = @client.options["vhost"]? || "/"
      name = ARGV.shift?
      pattern = ARGV.shift?
      definition = ARGV.shift?
      abort @parser.banner unless name && pattern && definition
      body = {
        pattern:    pattern,
        definition: JSON.parse(definition),
        "apply-to": @client.options["apply-to"]? || "all",
        "priority": @client.options["priority"]?.try &.to_i? || 0,
      }
      resp = @client.http.put "/api/policies/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}", @client.headers, body.to_json
      @client.handle_response(resp, 201, 204)
    end

    private def create_queue
      name = ARGV.shift?
      vhost = @client.options["vhost"]? || "/"
      abort @parser.banner unless name
      url = "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
      body = {
        "auto_delete": @client.options.has_key?("auto_delete"),
        "durable":     @client.options.has_key?("durable"),
        "arguments":   @parser.args,
      }
      resp = @client.http.put url, @client.headers, body.to_json
      @client.handle_response(resp, 201, 204)
    end

    private def delete_queue
      name = ARGV.shift?
      vhost = @client.options["vhost"]? || "/"
      abort @parser.banner unless name
      url = "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
      resp = @client.http.delete url
      @client.handle_response(resp, 204)
    end

    private def list_exchanges
      vhost = @client.options["vhost"]? || "/"
      puts "Listing exchanges for vhost #{vhost} ..." unless @client.quiet?

      ee = @client.get("/api/exchanges/#{URI.encode_www_form(vhost)}").map do |u|
        next unless e = u.as_h?
        {
          name: e["name"].to_s,
          type: e["type"].to_s,
        }
      end
      @client.output ee
    end

    private def create_exchange
      etype = ARGV.shift?
      name = ARGV.shift?
      vhost = @client.options["vhost"]? || "/"
      abort @parser.banner unless name && etype
      url = "/api/exchanges/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
      body = {
        "type":        etype,
        "auto_delete": @client.options.has_key?("auto_delete"),
        "durable":     @client.options.has_key?("durable"),
        "internal":    @client.options.has_key?("internal"),
        "delayed":     @client.options.has_key?("delayed"),
        "arguments":   @parser.args,
      }
      resp = @client.http.put url, @client.headers, body.to_json
      @client.handle_response(resp, 201, 204)
    end

    private def delete_exchange
      name = ARGV.shift?
      vhost = @client.options["vhost"]? || "/"
      abort @parser.banner unless name
      url = "/api/exchanges/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
      resp = @client.http.delete url
      @client.handle_response(resp, 204)
    end

    private def status
      resp = @client.http.get "/api/overview"
      @client.handle_response(resp, 200)
      body = JSON.parse(resp.body)
      status_obj = {
        Version:          body.dig("lavinmq_version"),
        Node:             body.dig("node"),
        Uptime:           body.dig("uptime"),
        Connections:      body.dig("object_totals", "connections"),
        Channels:         body.dig("object_totals", "channels"),
        Consumers:        body.dig("object_totals", "consumers"),
        Exchanges:        body.dig("object_totals", "exchanges"),
        Queues:           body.dig("object_totals", "queues"),
        Messages:         body.dig("queue_totals", "messages"),
        Messages_ready:   body.dig("queue_totals", "messages_ready"),
        Messages_unacked: body.dig("queue_totals", "messages_unacknowledged"),
      }
      @client.output(status_obj)
    end

    private def cluster_status
      resp = @client.http.get "/api/nodes"
      @client.handle_response(resp, 200)
      body = JSON.parse(resp.body)
      if followers = body[0].dig("followers").as_a
        cluster_status_obj = {
          this_node: body.dig(0, "name"),
          version:   body.dig(0, "applications", 0, "version"),
          followers: followers,
        }
        @client.output cluster_status_obj
      end
    end

    private def set_vhost_limits
      vhost = @client.options["vhost"]? || "/"
      data = ARGV.shift?
      abort @parser.banner unless data
      json = JSON.parse(data)
      ok = false
      if max_connections = json["max-connections"]?.try(&.as_i?)
        resp = @client.http.put "/api/vhost-limits/#{URI.encode_www_form(vhost)}/max-connections", @client.headers, {value: max_connections}.to_json
        @client.handle_response(resp, 204)
        ok = true
      end
      if max_queues = json["max-queues"]?.try(&.as_i?)
        resp = @client.http.put "/api/vhost-limits/#{URI.encode_www_form(vhost)}/max-queues", @client.headers, {value: max_queues}.to_json
        @client.handle_response(resp, 204)
        ok = true
      end
      ok || abort "max-queues or max-connections required"
    end

    private def set_permissions
      user = ARGV.shift?
      configure = ARGV.shift?
      write = ARGV.shift?
      read = ARGV.shift?
      vhost = @client.options["vhost"]? || "/"
      abort @parser.banner unless user && configure && read && write
      url = "/api/permissions/#{URI.encode_www_form(vhost)}/#{user}"
      body = {
        "configure": configure,
        "read":      read,
        "write":     write,
      }
      resp = @client.http.put url, @client.headers, body.to_json
      @client.handle_response(resp, 201, 204)
    end

    private def definitions
      data_dir = ARGV.shift? || abort "definitions <datadir>"
      DefinitionsGenerator.new(data_dir).generate(STDOUT)
    end

    private def hash_password
      password = ARGV.shift?
      abort @parser.banner unless password
      @client.output LavinMQ::Auth::User.hash_password(password, "SHA256")
    end
  end
end
