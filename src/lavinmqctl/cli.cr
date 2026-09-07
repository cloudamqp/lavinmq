require "http/client"
require "json"
require "option_parser"
require "../lavinmq/version"
require "../lavinmq/http/constants"
require "../lavinmq/shovel/constants"
require "../lavinmq/federation/constants"
require "../lavinmq/definitions_generator"
require "../lavinmq/auth/user"

class LavinMQCtl
  @options = {} of String => String
  @args = {} of String => JSON::Any
  @cmd : Proc(Nil)?
  @headers = HTTP::Headers{"Content-Type" => "application/json"}
  @parser = OptionParser.new
  @http : HTTP::Client?
  @io : IO
  @err_io : IO

  annotation Cmd; end
  annotation Opt; end

  SECTIONS = {"User Management", "Virtual Hosts", "Queues", "Exchanges",
              "Policies", "Connections", "Definitions", "Shovels",
              "Federation", "Server"}

  def initialize(@io : IO = STDOUT, @err_io : IO = STDERR)
    self.banner = "Usage: #{PROGRAM_NAME} [arguments] entity"
    if host = ENV["LAVINMQCTL_HOST"]?
      @options["host"] = host
    end
    if path = ENV["LAVINMQCTL_CONTROL_UNIX_PATH"]?
      @options["control_unix_path"] = path
    end
    global_options
    parse_cmd
  end

  def parse_cmd
    {% begin %}
      {%
        methods_by_section = {} of String => Object
        SECTIONS.each { |s| methods_by_section[s] = [] of Object }
        @type.methods.select(&.annotation(Cmd)).each do |m|
          cmd = m.annotation(Cmd)
          unless methods_by_section.has_key?(cmd[:section])
            cmd[:section].raise "Invalid section #{cmd[:section]}, must be one of #{SECTIONS}"
          end
          methods_by_section[cmd[:section]] << m
        end
      %}
      {% for section in SECTIONS %}
        {% section.raise "Section #{section} has no commands" if methods_by_section[section].empty? %}
        {% methods = methods_by_section[section].sort_by(&.name) %}
        @parser.separator({{ "\n" + section }})
        {% for method in methods %}
          {%
            cmd = method.annotation(Cmd)
            name = method.name.stringify
            description = cmd[0]
            usage = cmd[1]
          %}
          @parser.on({{ name }}, {{ description }}) do
            @cmd = ->{{ method.name.id }}
            self.banner = "Usage: #{PROGRAM_NAME} {{ method.name.id }} {{ usage.id }}"
            {% for opt in method.annotations(Opt).sort_by(&.[0]) %}
              {%
                flag = opt[0]
                desc = opt[1]
                options_key = opt[:options]
                args_key = opt[:args]
                value = opt[:value] || "v".id
              %}
              @parser.on({{ flag }}, {{ desc }}) do |v|
                {% if options_key %}
                  @options[{{ options_key }}] = {{ value }}
                {% else %}
                  @args[{{ args_key }}] = JSON::Any.new({{ value }})
                {% end %}
              end
            {% end %}
          end
        {% end %}
      {% end %}
    {% end %}
    @parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
  end

  def banner=(@banner : String)
    @parser.banner = @banner
  end

  # Override the top-level `abort` so error output goes through `@err_io`
  # (STDERR by default, a captured IO in specs) and can be redirected.
  private def abort(message = nil, status = 1) : NoReturn
    @err_io.puts message if message
    exit status
  end

  def run_cmd
    @parser.parse
    if cmd = @cmd
      cmd.call
    else
      @io.puts @parser
      abort
    end
  rescue ex : OptionParser::MissingOption
    abort ex
  rescue ex : IO::Error
    abort ex
  ensure
    @http.try(&.close)
  end

  private def connect
    if host = @options["host"]?
      validate_connection_args("host")
      client_from_uri(host)
    elsif uri = @options["uri"]?
      validate_connection_args("uri")
      client_from_uri(uri)
    elsif hostname = @options["hostname"]?
      scheme = @options["scheme"]? || "http"
      port = @options["port"]?.try &.to_i? || 15672
      uri = URI.new(scheme, hostname, port)
      client_from_uri(uri)
    else
      path = @options["control_unix_path"]? || LavinMQ::HTTP::DEFAULT_CONTROL_UNIX_PATH
      begin
        unless File.exists? path
          abort "#{path} not found. Is LavinMQ running?"
        end
        unless File::Info.writable? path
          abort "Please run lavinmqctl as root or as the same user as LavinMQ."
        end
        socket = UNIXSocket.new(path)
        HTTP::Client.new(socket)
      rescue ex : Socket::ConnectError
        abort "Can't connect to LavinMQ: #{ex.message}"
      end
    end
  end

  private def client_from_uri(uri : String)
    client_from_uri(URI.parse(uri))
  rescue ex : ArgumentError
    abort "Invalid URI. #{ex.message}"
  end

  private def client_from_uri(uri : URI)
    c = HTTP::Client.new(uri)
    uri.user = @options["user"] if @options["user"]?
    uri.password = @options["password"] if @options["password"]?
    c.basic_auth(uri.user, uri.password) if uri.user
    c
  end

  private def validate_connection_args(input_arg : String)
    invalid_args = Array(String).new
    invalid_args << "hostname" if @options["hostname"]?
    invalid_args << "port" if @options["port"]?
    invalid_args << "scheme" if @options["scheme"]?
    abort "invalid args when using #{input_arg}: #{invalid_args.join(", ")}" unless invalid_args.empty?
  end

  private def http
    @http ||= connect
  end

  private def global_options
    @parser.separator("\nGlobal options")
    @parser.on("-p vhost", "--vhost=vhost", "Specify vhost") do |v|
      @options["vhost"] = v
    end
    @parser.on("-H URI", "--host=URI", "Specify URI (Deprecated, use --uri or --hostname)") do |v|
      @options["host"] = v
    end
    @parser.on("-U URI", "--uri=URI", "Specify URI") do |v|
      @options["uri"] = v
    end
    @parser.on("--hostname=hostname", "Specify hostname") do |v|
      @options["hostname"] = v
    end
    @parser.on("--user=user", "Specify user") do |v|
      @options["user"] = v
    end
    @parser.on("--password=password", "Specify password") do |v|
      @options["password"] = v
    end
    @parser.on("-P port", "--port=port", "Specify port (15672)") do |v|
      @options["port"] = v
    end
    @parser.on("--scheme=scheme", "Specify scheme (http)") do |v|
      @options["scheme"] = v
    end
    @parser.on("--control-unix-path=PATH", "Path to the LavinMQ control socket (default: #{LavinMQ::HTTP::DEFAULT_CONTROL_UNIX_PATH})") do |v|
      @options["control_unix_path"] = v
    end
    @parser.on("-n node", "--node=node", "Specify node") do |v|
      # Only used by tests in cloudamqp/rabbitmq-java-client
      @options["node"] = v
    end
    @parser.on("-q", "--quiet", "suppress informational messages") do
      @options["quiet"] = "yes"
    end
    @parser.on("-s", "--silent", "suppress informational messages and table formatting") do
      @options["silent"] = "yes"
    end
    @parser.on("-f format", "--format=format", "Format output (text, json)") do |v|
      if v != "text" && v != "json"
        abort "Invalid format: #{v}"
      end
      @options["format"] = v
    end
    @parser.on("-v", "--version", "Show version") { @io.puts LavinMQ::VERSION; exit 0 }
    @parser.on("--build-info", "Show build information") { @io.puts LavinMQ::BUILD_INFO; exit 0 }
    @parser.on("-h", "--help", "Show this help") do
      @io.puts @parser
      exit 0
    end
  end

  private def quiet?
    @options["quiet"]? || @options["silent"]? || @options["format"]? == "json"
  end

  private def entity_arg
    entity = ARGV.shift?
    abort @banner unless entity && ENTITIES.includes?(entity)
    entity
  end

  private def handle_response(resp, *ok)
    return if ok.includes? resp.status_code
    if resp.status_code == 503
      output resp.body
      exit 2
    end
    output "#{resp.status_code} - #{resp.status}"
    output resp.body if resp.body? && !resp.headers["Content-Type"]?.try(&.starts_with?("text/html"))
    exit 1
  end

  private def output(data, columns = nil)
    if @options["format"]? == "json"
      data.to_json(@io)
      @io.puts
    else
      case data
      when Hash, NamedTuple
        data.each do |k, v|
          @io << k << ": " << v << "\n"
        end
      when Array
        output_array(data, columns)
      else
        @io.puts data
      end
    end
  end

  private def output_array(data : Array, columns : Array(String)?)
    first = data.first? || return

    headers = if columns
                columns
              else
                case first
                when NamedTuple then first.keys.to_a.map(&.to_s)
                when JSON::Any  then first.as_h.keys
                else                 [] of String
                end
              end

    if @options["silent"]?
      @io.puts headers.join("\t")
      data.each do |item|
        case item
        when Hash       then item.each_value.join(@io, "\t")
        when JSON::Any  then item.as_h.each_value.join(@io, "\t")
        when NamedTuple then item.values.join(@io, "\t")
        else                 item.to_s(@io)
        end
        @io.puts
      end
      return
    end

    rows = data.compact_map do |item|
      case item
      when NamedTuple
        values = [] of String
        item.each_value { |v| values << v.to_s }
        values
      when JSON::Any
        if h = item.as_h?
          columns ? columns.map { |c| item[c]?.try(&.to_s) || "" } : h.values.map(&.to_s)
        end
      when Hash
        item.values.map(&.to_s)
      else
        [item.to_s]
      end
    end

    render_table rows, headers
  end

  private def url_encoded_vhost
    URI.encode_www_form(@options["vhost"])
  end

  private def get(url, page = 1, items = Array(JSON::Any).new)
    resp = http.get("#{url}?page=#{page}&page_size=#{LavinMQ::HTTP::MAX_PAGE_SIZE}", @headers)
    handle_response(resp, 200)
    if data = JSON.parse(resp.body).as_h?
      items += data["items"].as_a
      page = data["page"].as_i
      if page < data["page_count"].as_i
        return get(url, page + 1, items)
      end
    else
      abort "Unexpected response from #{url}\n#{resp.body}"
    end
    items
  end

  @[Cmd("Import definitions in JSON", "<file>", section: "Definitions")]
  private def import_definitions
    file = ARGV.shift? || ""
    resp = if file == "-"
             http.post "/api/definitions", @headers, STDIN
           elsif File.exists?(file)
             File.open(file) do |io|
               http.post "/api/definitions", @headers, io
             end
           else
             STDERR.puts "ERROR: File not found"
             abort @banner
           end
    handle_response(resp, 200)
  end

  @[Cmd("Exports definitions in JSON", "", section: "Definitions")]
  private def export_definitions
    url = "/api/definitions"
    url += "/#{URI.encode_www_form(@options["vhost"])}" if @options.has_key?("vhost")
    resp = http.get url, @headers
    handle_response(resp, 200)
    output resp.body
  end

  @[Cmd("List user names and tags", "", section: "User Management")]
  private def list_users
    @io.puts "Listing users ..." unless quiet?
    uu = get("/api/users").map do |u|
      next unless user = u.as_h?
      {name: user["name"].to_s, tags: user["tags"].to_s}
    end
    output uu
  end

  @[Cmd("Creates a new user", "<username> <password>", section: "User Management")]
  private def add_user
    username = ARGV.shift?
    password = ARGV.shift?
    abort @banner unless username && password
    resp = http.put "/api/users/#{username}", @headers, {password: password}.to_json
    handle_response(resp, 201, 204)
  end

  @[Cmd("Delete a user", "<username>", section: "User Management")]
  private def delete_user
    username = ARGV.shift?
    abort @banner unless username
    resp = http.delete "/api/users/#{username}", @headers
    handle_response(resp, 204)
  end

  @[Cmd("Sets user tags", "<username> <tags>", section: "User Management")]
  private def set_user_tags
    username = ARGV.shift?
    tags = ARGV.join(",")
    abort @banner unless username && tags
    resp = http.put "/api/users/#{username}", @headers, {tags: tags}.to_json
    handle_response(resp, 201, 204)
  end

  @[Cmd("Change the user password", "<username> <new_password>", section: "User Management")]
  private def change_password
    username = ARGV.shift?
    pwd = ARGV.shift?
    abort @banner unless username && pwd
    resp = http.put "/api/users/#{username}", @headers, {password: pwd}.to_json
    handle_response(resp, 204)
  end

  @[Cmd("Lists queues and their properties", "", section: "Queues")]
  private def list_queues
    vhost = @options["vhost"]? || "/"
    @io.puts "Listing queues for vhost #{vhost} ..." unless quiet?
    qq = get("/api/queues/#{URI.encode_www_form(vhost)}").map do |u|
      next unless q = u.as_h?
      {name: q["name"].to_s, messages: q["messages"].to_s}
    end
    output qq
  end

  @[Cmd("Purges a queue (removes all messages in it)", "<queue>", section: "Queues")]
  private def purge_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.delete "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/contents", @headers
    handle_response(resp, 204)
  end

  @[Cmd("Pause all consumers on a queue", "<queue>", section: "Queues")]
  private def pause_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.put "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/pause", @headers
    handle_response(resp, 204)
  end

  @[Cmd("Resume all consumers on a queue", "<queue>", section: "Queues")]
  private def resume_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.put "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/resume", @headers
    handle_response(resp, 204)
  end

  @[Cmd("Restarts a closed queue", "<queue>", section: "Queues")]
  private def restart_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.put "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/restart", @headers
    handle_response(resp, 204)
  end

  @[Cmd("Lists AMQP 0.9.1 connections for the node", "", section: "Connections")]
  private def list_connections
    columns = ARGV
    columns = ["user", "peer_host", "peer_port", "state"] if columns.empty?
    conns = get("/api/connections")
    @io.puts "Listing connections ..." unless quiet?

    if @options["format"]? == "json"
      cc = conns.map do |u|
        next unless conn = u.as_h?
        conn.select { |k, _v| columns.includes? k }
      end
      output cc
    else
      output_array(conns, columns)
    end
  end

  private def print_erlang_terms(h : Hash)
    @io.print '['
    last_index = h.size - 1
    h.each_with_index do |(key, value), i|
      @io.print "{\"#{key}\","
      case value.raw
      when Hash   then print_erlang_terms(value.as_h)
      when String then @io.print '"', value, '"'
      else             @io.print value
      end
      @io.print '}'
      @io.print ',' unless i == last_index
    end
    @io.print ']'
  end

  @[Cmd("Instructs the broker to close a connection by pid", "<pid> <reason>", section: "Connections")]
  private def close_connection
    name = ARGV.shift?
    abort @banner unless name
    @io.puts "Closing connection #{name} ..." unless quiet?
    @headers["X-Reason"] = ARGV.shift? || "Closed via lavinmqctl"
    resp = http.delete "/api/connections/#{URI.encode_path(name)}", @headers
    handle_response(resp, 204)
  end

  @[Cmd("Instructs the broker to close all connections for the specified vhost or entire node", "<reason>", section: "Connections")]
  private def close_all_connections
    conns = get("/api/connections")
    closed_conns = [] of NamedTuple(name: String)
    @headers["X-Reason"] = ARGV.shift? || "Closed via lavinmqctl"
    conns.each do |u|
      next unless conn = u.as_h?
      name = conn["name"].to_s
      @io.puts "Closing connection #{name} ..." unless quiet?
      http.delete "/api/connections/#{URI.encode_path(name)}", @headers
      closed_conns << {name: name}
    end
    output closed_conns, ["closed_connections"]
  end

  @[Cmd("Lists virtual hosts", "", section: "Virtual Hosts")]
  private def list_vhosts
    @io.puts "Listing vhosts ..." unless quiet?
    vv = get("/api/vhosts").map do |u|
      next unless v = u.as_h?
      {name: v["name"].to_s}
    end
    output vv
  end

  @[Cmd("Creates a virtual host", "<vhost>", section: "Virtual Hosts")]
  private def add_vhost
    name = ARGV.shift?
    abort @banner unless name
    resp = http.put "/api/vhosts/#{URI.encode_www_form(name)}", @headers
    handle_response(resp, 201, 204)
  end

  @[Cmd("Deletes a virtual host", "<vhost>", section: "Virtual Hosts")]
  private def delete_vhost
    name = ARGV.shift?
    abort @banner unless name
    resp = http.delete "/api/vhosts/#{URI.encode_www_form(name)}", @headers
    handle_response(resp, 204)
  end

  @[Cmd("Clears (removes) a policy", "<name>", section: "Policies")]
  private def clear_policy
    vhost = @options["vhost"]? || "/"
    name = ARGV.shift?
    abort @banner unless name
    resp = http.delete "/api/policies/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}", @headers
    handle_response(resp, 204)
  end

  @[Cmd("Lists all policies in a virtual host", "", section: "Policies")]
  private def list_policies
    vhost = @options["vhost"]? || "/"
    @io.puts "Listing policies for vhost #{vhost} ..." unless quiet?
    output get("/api/policies/#{URI.encode_www_form(vhost)}")
  end

  @[Cmd("Sets or updates a policy", "<name> <pattern> <definition>", section: "Policies")]
  @[Opt("--priority=priority", "Specify priority", options: "priority")]
  @[Opt("--apply-to=apply-to", "Apply-to", options: "apply-to")]
  private def set_policy
    vhost = @options["vhost"]? || "/"
    name = ARGV.shift?
    pattern = ARGV.shift?
    definition = ARGV.shift?
    abort @banner unless name && pattern && definition
    body = {
      pattern:    pattern,
      definition: JSON.parse(definition),
      "apply-to": @options["apply-to"]? || "all",
      "priority": @options["priority"]?.try &.to_i? || 0,
    }
    resp = http.put "/api/policies/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}", @headers, body.to_json
    handle_response(resp, 201, 204)
  end

  @[Cmd("Create queue", "<name>", section: "Queues")]
  @[Opt("--auto-delete", "Auto delete queue when last consumer is removed", options: "auto_delete", value: "true")]
  @[Opt("--durable", "Make the queue durable", options: "durable", value: "true")]
  @[Opt("--expires", "", args: "x-expires", value: v.to_i64)]
  @[Opt("--max-length", "Set a max length for the queue", args: "x-max-length", value: v.to_i64)]
  @[Opt("--message-ttl", "Message time to live", args: "x-message-ttl", value: v.to_i64)]
  @[Opt("--delivery-limit", "How many time a message will be delivered before dead lettered", args: "x-delivery-limit", value: v.to_i64)]
  @[Opt("--reject-on-overflow", "Reject publish if max-length is met, otherwise messages in the queue is dropped", args: "x-overflow", value: "reject-publish")]
  @[Opt("--dead-letter-exchange", "To which exchange to dead letter messages", args: "x-dead-letter-exchange")]
  @[Opt("--dead-letter-routing-key", "Which routing key to use when dead lettering", args: "x-dead-letter-routing-key")]
  @[Opt("--stream-queue", "Create a Stream Queue", args: "x-queue-type", value: "stream")]
  private def create_queue
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    url = "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    body = {
      "auto_delete": @options.has_key?("auto_delete"),
      "durable":     @options.has_key?("durable"),
      "arguments":   @args,
    }
    resp = http.put url, @headers, body.to_json
    handle_response(resp, 201, 204)
  end

  @[Cmd("Delete queue", "<queue>", section: "Queues")]
  private def delete_queue
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    url = "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    resp = http.delete url
    handle_response(resp, 204)
  end

  @[Cmd("Lists exchanges", "", section: "Exchanges")]
  private def list_exchanges
    vhost = @options["vhost"]? || "/"
    @io.puts "Listing exchanges for vhost #{vhost} ..." unless quiet?

    ee = get("/api/exchanges/#{URI.encode_www_form(vhost)}").map do |u|
      next unless e = u.as_h?
      {
        name: e["name"].to_s,
        type: e["type"].to_s,
      }
    end
    output ee
  end

  @[Cmd("Create exchange", "<type> <name>", section: "Exchanges")]
  @[Opt("--auto-delete", "Auto delete exchange", options: "auto_delete", value: "true")]
  @[Opt("--durable", "Make the exchange durable", options: "durable", value: "true")]
  @[Opt("--internal", "Make the exchange internal", options: "internal", value: "true")]
  @[Opt("--delayed", "Make the exchange delayed", options: "delayed", value: "true")]
  @[Opt("--alternate-exchange", "Exchange to route all unroutable messages to", args: "x-alternate-exchange")]
  @[Opt("--persist-messages", "Number of messages to persist in the exchange", args: "x-persist-messages", value: v.to_i64)]
  @[Opt("--persist-ms", "Persist messages in the exchange for this amount of time", args: "x-persist-ms", value: v.to_i64)]
  private def create_exchange
    etype = ARGV.shift?
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name && etype
    url = "/api/exchanges/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    body = {
      "type":        etype,
      "auto_delete": @options.has_key?("auto_delete"),
      "durable":     @options.has_key?("durable"),
      "internal":    @options.has_key?("internal"),
      "delayed":     @options.has_key?("delayed"),
      "arguments":   @args,
    }
    resp = http.put url, @headers, body.to_json
    handle_response(resp, 201, 204)
  end

  @[Cmd("Delete exchange", "<name>", section: "Exchanges")]
  private def delete_exchange
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    url = "/api/exchanges/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    resp = http.delete url
    handle_response(resp, 204)
  end

  @[Cmd("Display server status", "", section: "Server")]
  private def status
    resp = http.get "/api/overview"
    handle_response(resp, 200)
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
      Bindings:         body.dig("object_totals", "bindings"),
      Messages:         body.dig("queue_totals", "messages"),
      Messages_ready:   body.dig("queue_totals", "messages_ready"),
      Messages_unacked: body.dig("queue_totals", "messages_unacknowledged"),
    }
    output(status_obj)
  end

  @[Cmd("Display cluster status", "", section: "Server")]
  private def cluster_status
    resp = http.get "/api/nodes"
    handle_response(resp, 200)
    body = JSON.parse(resp.body)
    if followers = body[0].dig("followers").as_a
      cluster_status_obj = {
        this_node: body.dig(0, "name"),
        version:   body.dig(0, "applications", 0, "version"),
        followers: followers,
      }
      output cluster_status_obj
    end
  end

  @[Cmd("Trigger a garbage collection cycle and print GC stats", "", section: "Server")]
  private def gc_collect
    resp = http.post "/api/nodes/gc_collect", @headers
    handle_response(resp, 204)
    resp = http.get "/api/nodes/gc_stats"
    handle_response(resp, 200)
    output JSON.parse(resp.body).as_h
  end

  @[Cmd("Stop the AMQP broker", "", section: "Server")]
  private def stop_app; end

  @[Cmd("Starts the AMQP broker", "", section: "Server")]
  private def start_app; end

  @[Cmd("Set VHost limits (max-connections, max-queues)", "<json>", section: "Virtual Hosts")]
  private def set_vhost_limits
    vhost = @options["vhost"]? || "/"
    data = ARGV.shift?
    abort @banner unless data
    json = JSON.parse(data)
    ok = false
    if max_connections = json["max-connections"]?.try(&.as_i?)
      resp = http.put "/api/vhost-limits/#{URI.encode_www_form(vhost)}/max-connections", @headers, {value: max_connections}.to_json
      handle_response(resp, 204)
      ok = true
    end
    if max_queues = json["max-queues"]?.try(&.as_i?)
      resp = http.put "/api/vhost-limits/#{URI.encode_www_form(vhost)}/max-queues", @headers, {value: max_queues}.to_json
      handle_response(resp, 204)
      ok = true
    end
    ok || abort "max-queues or max-connections required"
  end

  @[Cmd("Set permissions for a user", "<username> <configure> <write> <read>", section: "User Management")]
  private def set_permissions
    user = ARGV.shift?
    configure = ARGV.shift?
    write = ARGV.shift?
    read = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless user && configure && read && write
    url = "/api/permissions/#{URI.encode_www_form(vhost)}/#{user}"
    body = {
      "configure": configure,
      "read":      read,
      "write":     write,
    }
    resp = http.put url, @headers, body.to_json
    handle_response(resp, 201, 204)
  end

  @[Cmd("Generate definitions json from a data directory", "", section: "Definitions")]
  private def definitions
    data_dir = ARGV.shift? || abort "definitions <datadir>"
    DefinitionsGenerator.new(data_dir).generate(@io)
  end

  @[Cmd("Hash a password", "<password>", section: "User Management")]
  private def hash_password
    password = ARGV.shift?
    abort @banner unless password
    output LavinMQ::Auth::User.hash_password(password, "SHA256")
  end

  @[Cmd("Lists shovels", "", section: "Shovels")]
  private def list_shovels
    vhost = @options["vhost"]? || "/"
    @io.puts "Listing shovels for vhost #{vhost} ..." unless quiet?
    ss = get("/api/shovels/#{URI.encode_www_form(vhost)}").map do |s|
      next unless shovel = s.as_h?
      {
        name:  shovel["name"].to_s,
        vhost: shovel["vhost"].to_s,
        state: shovel["state"]?.try(&.to_s) || "N/A",
      }
    end
    output ss
  end

  @[Cmd("Create a shovel", "<name> --src-uri=<uri> --dest-uri=<uri>", section: "Shovels")]
  @[Opt("--src-uri=URI", "Source URI (required)", args: "src-uri")]
  @[Opt("--dest-uri=URI", "Destination URI (required)", args: "dest-uri")]
  @[Opt("--src-queue=QUEUE", "Source queue name", args: "src-queue")]
  @[Opt("--dest-queue=QUEUE", "Destination queue name", args: "dest-queue")]
  @[Opt("--src-exchange=EXCHANGE", "Source exchange name", args: "src-exchange")]
  @[Opt("--dest-exchange=EXCHANGE", "Destination exchange name", args: "dest-exchange")]
  @[Opt("--src-exchange-key=KEY", "Source routing key", args: "src-exchange-key")]
  @[Opt("--dest-exchange-key=KEY", "Destination routing key", args: "dest-exchange-key")]
  @[Opt("--src-prefetch-count=COUNT", "Source prefetch count", args: "src-prefetch-count", value: v.to_i64)]
  @[Opt("--src-delete-after=AFTER", "Delete after mode (never, queue-length, count)", args: "src-delete-after")]
  @[Opt("--ack-mode=MODE", "Acknowledgment mode (on-confirm, on-publish, no-ack)", args: "ack-mode")]
  @[Opt("--reconnect-delay=SECONDS", "Reconnect delay in seconds", args: "reconnect-delay", value: v.to_i64)]
  private def add_shovel
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    abort "Fields '--src-uri' and '--dest-uri' are required" unless @args["src-uri"]? && @args["dest-uri"]?

    # Set default values if not provided
    @args["src-prefetch-count"] ||= JSON::Any.new(LavinMQ::Shovel::DEFAULT_PREFETCH.to_i64)
    @args["reconnect-delay"] ||= JSON::Any.new(LavinMQ::Shovel::DEFAULT_RECONNECT_DELAY.total_seconds.to_i64)
    @args["ack-mode"] ||= JSON::Any.new(LavinMQ::Shovel::DEFAULT_ACK_MODE.to_s.underscore.gsub("_", "-"))
    @args["src-delete-after"] ||= JSON::Any.new(LavinMQ::Shovel::DEFAULT_DELETE_AFTER.to_s.underscore.gsub("_", "-"))

    url = "/api/parameters/shovel/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    body = {"value" => @args}
    resp = http.put url, @headers, body.to_json
    handle_response(resp, 201, 204)
  end

  @[Cmd("Delete a shovel", "<name>", section: "Shovels")]
  private def delete_shovel
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    url = "/api/parameters/shovel/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    resp = http.delete url
    handle_response(resp, 204)
  end

  @[Cmd("Lists federation upstreams", "", section: "Federation")]
  private def list_federations
    vhost = @options["vhost"]? || "/"
    @io.puts "Listing federation upstreams for vhost #{vhost} ..." unless quiet?
    ff = get("/api/parameters/federation-upstream/#{URI.encode_www_form(vhost)}").map do |u|
      next unless f = u.as_h?
      {name: f["name"].to_s, component: f["component"].to_s}
    end
    output ff
  end

  @[Cmd("Create a federation upstream", "<name> --uri=<uri>", section: "Federation")]
  @[Opt("--uri=URI", "Upstream URI (required)", args: "uri")]
  @[Opt("--expires=SECONDS", "Expiry time for federation link", args: "expires", value: v.to_i64)]
  @[Opt("--message-ttl=MILLISECONDS", "Message TTL for federation", args: "message-ttl", value: v.to_i64)]
  @[Opt("--max-hops=COUNT", "Maximum hops for federation", args: "max-hops", value: v.to_i64)]
  @[Opt("--prefetch-count=COUNT", "Prefetch count for federation", args: "prefetch-count", value: v.to_i64)]
  @[Opt("--reconnect-delay=SECONDS", "Reconnect delay in seconds", args: "reconnect-delay", value: v.to_i64)]
  @[Opt("--ack-mode=MODE", "Acknowledgment mode (on-confirm, on-publish, no-ack)", args: "ack-mode")]
  @[Opt("--consumer-tag=TAG", "Consumer tag for federation link", args: "consumer-tag")]
  @[Opt("--exchange=EXCHANGE", "Exchange name to federate", args: "exchange")]
  @[Opt("--queue=QUEUE", "Queue name to federate", args: "queue")]
  private def add_federation
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    abort "Field '--uri' is required" unless @args["uri"]?

    # Set default values if not provided
    @args["prefetch-count"] ||= JSON::Any.new(LavinMQ::Federation::DEFAULT_PREFETCH.to_i64)
    @args["reconnect-delay"] ||= JSON::Any.new(LavinMQ::Federation::DEFAULT_RECONNECT_DELAY.total_seconds)
    @args["ack-mode"] ||= JSON::Any.new(LavinMQ::Federation::DEFAULT_ACK_MODE.to_s.underscore.gsub("_", "-"))
    @args["max-hops"] ||= JSON::Any.new(LavinMQ::Federation::DEFAULT_MAX_HOPS)

    url = "/api/parameters/federation-upstream/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    body = {"value" => @args}
    resp = http.put url, @headers, body.to_json
    handle_response(resp, 201, 204)
  end

  @[Cmd("Delete a federation upstream", "<name>", section: "Federation")]
  private def delete_federation
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    url = "/api/parameters/federation-upstream/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    resp = http.delete url
    handle_response(resp, 204)
  end

  private def render_table(rows : Array(Array(String)), headers : Array(String))
    widths = headers.map(&.size)
    rows.each do |row|
      row.each_with_index do |cell, i|
        widths[i] = {widths[i], cell.size}.max if i < widths.size
      end
    end
    table_line("┌", "─", "┬", "┐", widths)
    table_row(headers, widths)
    table_line("├", "─", "┼", "┤", widths)
    rows.each_with_index do |row, i|
      table_row(row, widths)
      if i < rows.size - 1
        table_line("├", "─", "┼", "┤", widths)
      end
    end
    table_line("└", "─", "┴", "┘", widths)
  end

  private def table_line(left : String, fill : String, sep : String, right : String, widths : Array(Int32))
    @io << left
    widths.each_with_index do |w, i|
      @io << fill * (w + 2)
      @io << (i < widths.size - 1 ? sep : right)
    end
    @io.puts
  end

  private def table_row(cells : Array(String), widths : Array(Int32))
    @io << "│"
    cells.each_with_index do |cell, i|
      @io << " " << cell.ljust(widths[i]) << " │"
    end
    @io.puts
  end
end
