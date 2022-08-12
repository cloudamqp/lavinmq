require "./lavinmq/version"
require "./lavinmq/config"
require "./lavinmq/http/constants"
require "http/client"
require "json"
require "option_parser"

class LavinMQCtl
  @@cfg = LavinMQ::Config.instance
  @options = {} of String => String
  @args = {} of String => JSON::Any
  @cmd : String?
  @headers = HTTP::Headers{"Content-Type" => "application/json"}
  @parser = OptionParser.new
  @http : HTTP::Client?
  @socket : UNIXSocket?

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
  }

  def initialize
    self.banner = "Usage: #{PROGRAM_NAME} [arguments] entity"
    global_options
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
    @parser.on("reset_vhost", "Purges all messages in all queues in the vhost and close all consumers, optionally back up the data") do
      @cmd = "reset_vhost"
      self.banner = "Usage: #{PROGRAM_NAME} reset_vhost <vhost>"
      @parser.on("--backup-data", "Backup the vhost data folder") do
        @options["backup"] = "true"
      end
      @parser.on("--backup-dir-name=", "Name of the backup folder, defaults to current unix timestamp") do |v|
        @options["backup-dir-name"] = v
      end
    end
    @parser.on("status", "Display server status") do
      @cmd = "status"
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
    when "reset_vhost"           then reset_vhost
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
    when "set_vhost_limits"      then set_vhost_limits
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
    @socket.try(&.close)
  end

  private def connect
    if host = @options["host"]?
      uri = URI.parse(host)
      c = HTTP::Client.new(uri)
      c.basic_auth(uri.user, uri.password) if uri.user
      c
    else
      socket = UNIXSocket.new(LavinMQ::HTTP::INTERNAL_UNIX_SOCKET)
      @socket = socket
      HTTP::Client.new(socket, @@cfg.http_bind, @@cfg.http_port)
    end
  end

  private def http
    @http ||= connect
  end

  private def global_options
    @parser.on("-p vhost", "--vhost=vhost", "Specify vhost") do |v|
      @options["vhost"] = v
    end
    @parser.on("-H host", "--host=host", "Specify host") do |v|
      @options["host"] = v
    end
    @parser.on("-n node", "--node=node", "Specify node") do |v|
      @options["node"] = v
    end
    @parser.on("-q", "--quiet", "suppress informational messages") do |_|
      @options["quiet"] = "yes"
    end
    @parser.on("-s", "--silent", "suppress informational messages and table formatting") do |_|
      @options["silent"] = "yes"
    end
  end

  private def quiet?
    @options["quiet"]? || @options["silent"]?
  end

  private def entity_arg
    entity = ARGV.shift?
    abort @banner unless entity && ENTITIES.includes?(entity)
    entity
  end

  private def handle_response(resp, *ok)
    return if ok.includes? resp.status_code
    case resp.status_code
    when 401
      puts "Access denied"
    when 404
      puts "Not found"
    else
      case resp.headers["Content-type"]?
      when "application/json"
        begin
          body = JSON.parse(resp.body)
          puts body["reason"]?
        rescue e : JSON::ParseException # Body can be empty
        end
      else
        puts resp.body
      end
    end
    exit 1
  end

  private def url_encoded_vhost
    URI.encode_www_form(@options["vhost"])
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
    print resp.body
  end

  private def list_users
    resp = http.get "/api/users", @headers
    handle_response(resp, 200)
    puts "Listing users ..." unless quiet?
    columns = %w[name tags]
    puts columns.join("\t")
    if users = JSON.parse(resp.body).as_a?
      users.each do |u|
        next unless user = u.as_h?
        puts columns.map { |c| user[c] }.join("\t")
      end
    end
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
    tags = ARGV.shift?
    abort @banner unless username && tags
    resp = http.put "/api/users/#{username}", @headers, {tags: tags}.to_json
    handle_response(resp, 204)
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
    resp = http.get "/api/queues/#{URI.encode_www_form(vhost)}", @headers
    puts "Listing queues for vhost #{vhost} ..." unless quiet?
    handle_response(resp, 200)
    if queues = JSON.parse(resp.body).as_a?
      puts "name\tmessages"
      queues.each do |u|
        next unless q = u.as_h?
        puts "#{q["name"]}\t#{q["messages"]}"
      end
      true
    else
      abort "invalid data"
    end
  end

  private def purge_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.delete "/api/queues/#{URI.encode_www_form(vhost)}/#{queue}/contents", @headers
    handle_response(resp, 204)
  end

  private def pause_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.put "/api/queues/#{URI.encode_www_form(vhost)}/#{queue}/pause", @headers
    handle_response(resp, 204)
  end

  private def resume_queue
    vhost = @options["vhost"]? || "/"
    queue = ARGV.shift?
    abort @banner unless queue
    resp = http.put "/api/queues/#{URI.encode_www_form(vhost)}/#{queue}/resume", @headers
    handle_response(resp, 204)
  end

  private def list_connections
    columns = ARGV
    columns = ["user", "peer_host", "peer_port", "state"] if columns.empty?
    resp = http.get "/api/connections", @headers
    puts "Listing connections ..." unless quiet?
    handle_response(resp, 200)
    if conns = JSON.parse(resp.body).as_a?
      puts columns.join("\t")
      conns.each do |u|
        next unless conn = u.as_h?
        puts columns.map { |c| conn[c] }.join("\t")
      end
    else
      abort "invalid data"
    end
  end

  private def close_connection
    name = ARGV.shift?
    abort @banner unless name
    puts "Closing connection #{name} ..." unless quiet?
    @headers["X-Reason"] = ARGV.shift? || "CONNECTION_FORCED - Closed via lavinmqctl"
    resp = http.delete "/api/connections/#{URI.encode_path(name)}", @headers
    handle_response(resp, 204)
  end

  private def close_all_connections
    resp = http.get "/api/connections", @headers
    handle_response(resp, 200)
    if conns = JSON.parse(resp.body).as_a?
      @headers["X-Reason"] = ARGV.shift? || "CONNECTION_FORCED - Closed via lavinmqctl"
      conns.each do |u|
        next unless conn = u.as_h?
        name = conn["name"].to_s
        puts "Closing connection #{name} ..." unless quiet?
        http.delete "/api/connections/#{URI.encode_path(name)}", @headers
      end
    else
      abort "invalid data"
    end
  end

  private def list_vhosts
    resp = http.get "/api/vhosts", @headers
    puts "Listing vhosts ..." unless quiet?
    handle_response(resp, 200)
    if vhosts = JSON.parse(resp.body).as_a?
      puts "name"
      vhosts.each do |u|
        next unless v = u.as_h?
        puts "#{v["name"]}"
      end
    else
      abort "invalid data"
    end
  end

  private def add_vhost
    name = ARGV.shift?
    abort @banner unless name
    resp = http.put "/api/vhosts/#{name}", @headers
    handle_response(resp, 204)
  end

  private def delete_vhost
    name = ARGV.shift?
    abort @banner unless name
    resp = http.delete "/api/vhosts/#{name}", @headers
    handle_response(resp, 204)
  end

  private def reset_vhost
    name = ARGV.shift?
    abort @banner unless name
    body = if @options.has_key? "backup"
             dir_name = @options["backup-dir-name"]?.to_s
             dir_name = Time.utc.to_unix.to_s if dir_name.empty?
             {
               "backup":          true,
               "backup_dir_name": dir_name,
             }
           end
    body ||= {} of String => String
    resp = http.post "/api/vhosts/#{name}/purge_and_close_consumers", @headers, body.to_json
    handle_response(resp, 204)
  end

  private def clear_policy
    vhost = @options["vhost"]? || "/"
    name = ARGV.shift?
    abort @banner unless name
    resp = http.delete "/api/policies/#{URI.encode_www_form(vhost)}/#{name}", @headers
    handle_response(resp, 204)
  end

  private def list_policies
    vhost = @options["vhost"]? || "/"
    resp = http.get "/api/policies/#{URI.encode_www_form(vhost)}", @headers
    puts "Listing policies for vhost #{vhost} ..." unless quiet?
    handle_response(resp, 200)
    if policies = JSON.parse(resp.body).as_a?
      puts "vhost\tname\tpattern\tapply-to\tdefinition\tpriority"
      policies.each do |u|
        next unless p = u.as_h?
        puts "#{p["vhost"]}\t#{p["name"]}\t#{p["pattern"]}\t#{p["apply-to"]}\t#{p["definition"]}\t#{p["priority"]}"
      end
    else
      abort "invalid data"
    end
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
    resp = http.put "/api/policies/#{URI.encode_www_form(vhost)}/#{name}", @headers, body.to_json
    handle_response(resp, 201, 204)
  end

  private def create_queue
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name
    url = "/api/queues/#{URI.encode_www_form(vhost)}/#{name}"
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
    url = "/api/queues/#{URI.encode_www_form(vhost)}/#{name}"
    resp = http.delete url
    handle_response(resp, 204)
  end

  private def list_exchanges
    vhost = @options["vhost"]? || "/"
    resp = http.get "/api/exchanges/#{URI.encode_www_form(vhost)}", @headers
    puts "Listing exchanges for vhost #{vhost} ..." unless quiet?
    handle_response(resp, 200)
    columns = %w[name type]
    puts columns.join("\t")
    if exchanges = JSON.parse(resp.body).as_a?
      exchanges.each do |e|
        next unless ex = e.as_h?
        columns.each { |c| print ex[c]; print "\t" }
      end
    else
      abort "invalid data"
    end
  end

  private def create_exchange
    etype = ARGV.shift?
    name = ARGV.shift?
    vhost = @options["vhost"]? || "/"
    abort @banner unless name && etype
    url = "/api/exchanges/#{URI.encode_www_form(vhost)}/#{name}"
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
    url = "/api/exchanges/#{URI.encode_www_form(vhost)}/#{name}"
    resp = http.delete url
    handle_response(resp, 204)
  end

  private def status
    resp = http.get "/api/overview"
    handle_response(resp, 200)
    body = JSON.parse(resp.body)
    puts "Version: #{body.dig("lavinmq_version")}"
    puts "Node: #{body.dig("node")}"
    puts "Uptime: #{body.dig("uptime")}"
    puts "Connections: #{body.dig("object_totals", "connections")}"
    puts "Channels: #{body.dig("object_totals", "channels")}"
    puts "Consumers: #{body.dig("object_totals", "consumers")}"
    puts "Exchanges: #{body.dig("object_totals", "exchanges")}"
    puts "Queues: #{body.dig("object_totals", "queues")}"
    puts "Messages: #{body.dig("queue_totals", "messages")}"
    puts "Messages ready: #{body.dig("queue_totals", "messages_ready")}"
    puts "Messages unacked: #{body.dig("queue_totals", "messages_unacknowledged")}"
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
end

cli = LavinMQCtl.new
cli.run_cmd
