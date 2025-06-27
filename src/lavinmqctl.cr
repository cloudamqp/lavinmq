require "./lavinmq/version"
require "./lavinmq/http/constants"
require "./lavinmq/definitions_generator"
require "http/client"
require "json"
require "option_parser"
require "./lavinmq/auth/user"

class LavinMQCtl
  @options = {} of String => String
  @args = {} of String => JSON::Any
  @cmd : String?
  @headers = HTTP::Headers{"Content-Type" => "application/json"}
  @parser = OptionParser.new
  @http : HTTP::Client?

  COMPAT_CMDS = {
    {"add_user", "Creates a new user", "<username> <password>"},
    {"change_password", "Change the user password", "<username> <new_password>"},
    {"delete_user", "Delete a user", "<username>"},
    {"list_users", "List user names and tags", ""},
    {"set_user_tags", "Sets user tags", "<username> <tags>"},
    {"list_vhosts", "Lists virtual hosts", ""},
    {"add_vhost", "Creates a virtual host", "<vhost>"},
    {"delete_vhost", "Deletes a virtual host", "<vhost>"},
    {"clear_policy", "Clears (removes) a policy", "<name>"},
    {"list_policies", "Lists all policies in a virtual host", ""},
    {"list_connections", "Lists AMQP 0.9.1 connections for the node", ""},
    {"list_queues", "Lists queues and their properties", ""},
    {"purge_queue", "Purges a queue (removes all messages in it)", "<queue>"},
    {"pause_queue", "Pause all consumers on a queue", "<queue>"},
    {"resume_queue", "Resume all consumers on a queue", "<queue>"},
    {"delete_queue", "Delete queue", "<queue>"},
    {"export_definitions", "Exports definitions in JSON", ""},
    {"import_definitions", "Import definitions in JSON", "<file>"},
    {"close_all_connections", "Instructs the broker to close all connections for the specified vhost or entire node", "<reason>"},
    {"close_connection", "Instructs the broker to close a connection by pid", "<pid> <reason>"},
    {"stop_app", "Stop the AMQP broker", ""},
    {"start_app", "Starts the AMQP broker", ""},

    {"list_exchanges", "Lists exchanges", ""},
    {"delete_exchange", "Delete exchange", "<name>"},
    {"set_vhost_limits", "Set VHost limits (max-connections, max-queues)", "<json>"},
    {"set_permissions", "Set permissions for a user", "<username> <configure> <write> <read>"},
    {"hash_password", "Hash a password", "<password>"},

  }

  def initialize
    self.banner = "Usage: #{PROGRAM_NAME} [arguments] entity"
    if host = ENV["LAVINMQCTL_HOST"]?
      @options["host"] = host
    end
    global_options
    parse_cmd
  end

  def parse_cmd
    @parser.separator("\nCommands:")
    COMPAT_CMDS.each do |cmd|
      @parser.on(cmd[0], cmd[1]) do
        @cmd = cmd[0]
        self.banner = "Usage: #{PROGRAM_NAME} #{cmd[0]} #{cmd[2]}"
      end
    end
    @parser.on("set_policy", "Sets or updates a policy") do
      @cmd = "set_policy"
      self.banner = "Usage: #{PROGRAM_NAME} set_policy <name> <pattern> <definition>"
      @parser.on("--priority=priority", "Specify priority") do |v|
        @options["priority"] = v
      end
      @parser.on("--apply-to=apply-to", "Apply-to") do |v|
        @options["apply-to"] = v
      end
    end
    @parser.on("create_queue", "Create queue") do
      @cmd = "create_queue"
      self.banner = "Usage: #{PROGRAM_NAME} create_queue <name>"
      @parser.on("--auto-delete", "Auto delete queue when last consumer is removed") do
        @options["auto_delete"] = "true"
      end
      @parser.on("--durable", "Make the queue durable") do
        @options["durable"] = "true"
      end
      @parser.on("--expires", "") do |v|
        @args["x-expires"] = JSON::Any.new(v.to_i64)
      end
      @parser.on("--max-length", "Set a max length for the queue") do |v|
        @args["x-max-length"] = JSON::Any.new(v.to_i64)
      end
      @parser.on("--message-ttl", "Message time to live") do |v|
        @args["x-message-ttl"] = JSON::Any.new(v.to_i64)
      end
      @parser.on("--delivery-limit", "How many time a message will be delivered before dead lettered") do |v|
        @args["x-delivery-limit"] = JSON::Any.new(v.to_i64)
      end
      @parser.on("--reject-on-overflow", "Reject publish if max-length is met, otherwise messages in the queue is dropped") do
        @args["x-overflow"] = JSON::Any.new("reject-publish")
      end
      @parser.on("--dead-letter-exchange", "To which exchange to dead letter messages") do |v|
        @args["x-dead-letter-exchange"] = JSON::Any.new(v)
      end
      @parser.on("--dead-letter-routing-key", "Which routing key to use when dead lettering") do |v|
        @args["x-dead-letter-routing-key"] = JSON::Any.new(v)
      end
      @parser.on("--stream-queue", "Create a Stream Queue") do
        @args["x-queue-type"] = JSON::Any.new("stream")
      end
    end
    @parser.on("create_exchange", "Create exchange") do
      @cmd = "create_exchange"
      self.banner = "Usage: #{PROGRAM_NAME} create_exchange <type> <name>"
      @parser.on("--auto-delete", "Auto delete exchange") do
        @options["auto_delete"] = "true"
      end
      @parser.on("--durable", "Make the exchange durable") do
        @options["durable"] = "true"
      end
      @parser.on("--internal", "Make the exchange internal") do
        @options["durable"] = "true"
      end
      @parser.on("--delayed", "Make the exchange delayed") do
        @options["delayed"] = "true"
      end
      @parser.on("--alternate-exchange", "Exchange to route all unroutable messages to") do |v|
        @args["x-alternate-exchange"] = JSON::Any.new(v)
      end
      @parser.on("--persist-messages", "Number of messages to persist in the exchange") do |v|
        @args["x-persist-messages"] = JSON::Any.new(v.to_i64)
      end
      @parser.on("--persist-ms", "Persist messages in the exchange for this amount of time") do |v|
        @args["x-persist-ms"] = JSON::Any.new(v.to_i64)
      end
    end
    @parser.on("status", "Display server status") do
      @cmd = "status"
    end
    @parser.on("cluster_status", "Display cluster status") do
      @cmd = "cluster_status"
    end
    @parser.on("definitions", "Generate definitions json from a data directory") do
      @cmd = "definitions"
    end
    @parser.on("-v", "--version", "Show version") { puts LavinMQ::VERSION; exit 0 }
    @parser.on("--build-info", "Show build information") { puts LavinMQ::BUILD_INFO; exit 0 }
    @parser.on("-h", "--help", "Show this help") do
      puts @parser
      exit 1
    end
    @parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
  end

  def banner=(@banner : String)
    @parser.banner = @banner
  end

  # ameba:disable Metrics/CyclomaticComplexity
  def run_cmd
    @parser.parse
    case @cmd
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
      puts @parser
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
      begin
        unless File.exists? LavinMQ::HTTP::INTERNAL_UNIX_SOCKET
          abort "#{LavinMQ::HTTP::INTERNAL_UNIX_SOCKET} not found. Is LavinMQ running?"
        end
        unless File::Info.writable? LavinMQ::HTTP::INTERNAL_UNIX_SOCKET
          abort "Please run lavinmqctl as root or as the same user as LavinMQ."
        end
        socket = UNIXSocket.new(LavinMQ::HTTP::INTERNAL_UNIX_SOCKET)
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
    @parser.separator("\nGlobal options:")
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
      data.to_json(STDOUT)
      puts
    else
      case data
      when Hash, NamedTuple
        data.each do |k, v|
          STDOUT << k << ": " << v << "\n"
        end
      when Array
        output_array(data, columns)
      else
        puts data
      end
    end
  end

  private def output_array(data : Array, columns : Array(String)?)
    if columns
      puts columns.join(STDOUT, "\t")
    else
      case first = data.first?
      when NamedTuple
        puts first.keys.join(STDOUT, "\t")
      when JSON::Any
        puts first.as_h.each_key.join(STDOUT, "\t")
      end
    end
    data.each do |item|
      case item
      when Hash
        item.each_value.join(STDOUT, "\t")
      when JSON::Any
        item.as_h.each_value.join(STDOUT, "\t")
      when NamedTuple
        item.values.join(STDOUT, "\t")
      else
        item.to_s(STDOUT)
      end
      puts
    end
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

  private def export_definitions
    url = "/api/definitions"
    url += "/#{URI.encode_www_form(@options["vhost"])}" if @options.has_key?("vhost")
    resp = http.get url, @headers
    handle_response(resp, 200)
    output resp.body
  end

  private def list_users
    puts "Listing users ..." unless quiet?
    uu = get("/api/users").map do |u|
      next unless user = u.as_h?
      {name: user["name"].to_s, tags: user["tags"].to_s}
    end
    output uu
  end

  private def add_user
    username = ARGV.shift?
    password = ARGV.shift?
    abort @banner unless username && password
    resp = http.put "/api/users/#{username}", @headers, {password: password}.to_json
    handle_response(resp, 201, 204)
  end

  private def delete_user
    username = ARGV.shift?
    abort @banner unless username
    resp = http.delete "/api/users/#{username}", @headers
    handle_response(resp, 204)
  end

  private def set_user_tags
    username = ARGV.shift?
    tags = ARGV.join(",")
    abort @banner unless username && tags
    resp = http.put "/api/users/#{username}", @headers, {tags: tags}.to_json
    handle_response(resp, 201, 204)
  end

  private def change_password
    username = ARGV.shift?
    pwd = ARGV.shift?
    abort @banner unless username && pwd
    resp = http.put "/api/users/#{username}", @headers, {password: pwd}.to_json
    handle_response(resp, 204)
  end

  private def list_queues
    vhost = @options["vhost"]? || "/"
    puts "Listing queues for vhost #{vhost} ..." unless quiet?
    qq = get("/api/queues/#{URI.encode_www_form(vhost)}").map do |u|
      next unless q = u.as_h?
      {name: q["name"].to_s, messages: q["messages"].to_s}
    end
    output qq
  end

  private def purge_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.delete "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/contents", @headers
    handle_response(resp, 204)
  end

  private def pause_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.put "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/pause", @headers
    handle_response(resp, 204)
  end

  private def resume_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.put "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(queue)}/resume", @headers
    handle_response(resp, 204)
  end

  private def list_connections
    columns = ARGV
    columns = ["user", "peer_host", "peer_port", "state"] if columns.empty?
    conns = get("/api/connections")
    puts "Listing connections ..." unless quiet?

    if @options["format"]? == "json"
      cc = conns.map do |u|
        next unless conn = u.as_h?
        conn.select { |k, _v| columns.includes? k }
      end
      output cc
    else
      puts columns.join(STDOUT, "\t")
      conns.each do |u|
        if conn = u.as_h?
          columns.each_with_index do |c, i|
            case c
            when "client_properties"
              print_erlang_terms(conn[c].as_h)
            else
              print conn[c]
            end
            print "\t" unless i == columns.size - 1
          end
          puts
        end
      end
    end
  end

  private def print_erlang_terms(h : Hash)
    print '['
    last_index = h.size - 1
    h.each_with_index do |(key, value), i|
      print "{\"#{key}\","
      case value.raw
      when Hash   then print_erlang_terms(value.as_h)
      when String then print '"', value, '"'
      else             print value
      end
      print '}'
      print ',' unless i == last_index
    end
    print ']'
  end

  private def close_connection
    name = ARGV.shift?
    abort @banner unless name
    puts "Closing connection #{name} ..." unless quiet?
    @headers["X-Reason"] = ARGV.shift? || "Closed via lavinmqctl"
    resp = http.delete "/api/connections/#{URI.encode_path(name)}", @headers
    handle_response(resp, 204)
  end

  private def close_all_connections
    conns = get("/api/connections")
    closed_conns = [] of NamedTuple(name: String)
    @headers["X-Reason"] = ARGV.shift? || "Closed via lavinmqctl"
    conns.each do |u|
      next unless conn = u.as_h?
      name = conn["name"].to_s
      puts "Closing connection #{name} ..." unless quiet?
      http.delete "/api/connections/#{URI.encode_path(name)}", @headers
      closed_conns << {name: name}
    end
    output closed_conns, ["closed_connections"]
  end

  private def list_vhosts
    puts "Listing vhosts ..." unless quiet?
    vv = get("/api/vhosts").map do |u|
      next unless v = u.as_h?
      {name: v["name"].to_s}
    end
    output vv
  end

  private def add_vhost
    name = ARGV.shift?
    abort @banner unless name
    resp = http.put "/api/vhosts/#{URI.encode_www_form(name)}", @headers
    handle_response(resp, 201, 204)
  end

  private def delete_vhost
    name = ARGV.shift?
    abort @banner unless name
    resp = http.delete "/api/vhosts/#{URI.encode_www_form(name)}", @headers
    handle_response(resp, 204)
  end

  private def clear_policy
    vhost = @options["vhost"]? || "/"
    name = ARGV.shift?
    abort @banner unless name
    resp = http.delete "/api/policies/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}", @headers
    handle_response(resp, 204)
  end

  private def list_policies
    vhost = @options["vhost"]? || "/"
    puts "Listing policies for vhost #{vhost} ..." unless quiet?
    output get("/api/policies/#{URI.encode_www_form(vhost)}")
  end

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

  private def delete_queue
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    url = "/api/queues/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    resp = http.delete url
    handle_response(resp, 204)
  end

  private def list_exchanges
    vhost = @options["vhost"]? || "/"
    puts "Listing exchanges for vhost #{vhost} ..." unless quiet?

    ee = get("/api/exchanges/#{URI.encode_www_form(vhost)}").map do |u|
      next unless e = u.as_h?
      {
        name: e["name"].to_s,
        type: e["type"].to_s,
      }
    end
    output ee
  end

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

  private def delete_exchange
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    url = "/api/exchanges/#{URI.encode_www_form(vhost)}/#{URI.encode_www_form(name)}"
    resp = http.delete url
    handle_response(resp, 204)
  end

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
      Messages:         body.dig("queue_totals", "messages"),
      Messages_ready:   body.dig("queue_totals", "messages_ready"),
      Messages_unacked: body.dig("queue_totals", "messages_unacknowledged"),
    }
    output(status_obj)
  end

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

  private def definitions
    data_dir = ARGV.shift? || abort "definitions <datadir>"
    DefinitionsGenerator.new(data_dir).generate(STDOUT)
  end

  private def hash_password
    password = ARGV.shift?
    abort @banner unless password
    output LavinMQ::User.hash_password(password, "SHA256")
  end
end

cli = LavinMQCtl.new
cli.run_cmd
