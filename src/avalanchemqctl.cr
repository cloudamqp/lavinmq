require "./avalanchemq/version"
require "./avalanchemq/config"
require "./avalanchemq/http/http_server"
require "./stdlib/http_client"
require "json"
require "option_parser"

class AvalancheMQCtl
  @@cfg = AvalancheMQ::Config.instance
  @options = {} of String => String
  @cmd : String?
  @headers = HTTP::Headers{"Content-Type" => "application/json"}
  @parser = OptionParser.new
  @http : HTTP::Client?
  @socket : UNIXSocket?

  # Subcommands
  REMOVE        = "remove"
  CREATE        = "create"
  LIST          = "list"
  ENTITIES = %w[queues exchanges vhosts policies users]

  COMPAT_CMDS = {
    {"add_user",                "Creates a new user", "<username> <password>"},
    {"change_password",         "Change the user password", "<username> <new_password>"},
    {"delete_user",             "Delete a user", "<username>"},
    {"list_users",              "List user names and tags", ""},
    {"set_user_tags",           "Sets user tags", "<username> <tags>"},
    {"list_vhosts",             "Lists virtual hosts", ""},
    {"add_vhost",               "Creates a virtual host", "<vhost>"},
    {"delete_vhost",            "Deletes a virtual host", "<vhost>"},
    {"clear_policy",            "Clears (removes) a policy", "<name>"},
    {"list_policies",           "Lists all policies in a virtual host", ""},
    {"list_connections",        "Lists AMQP 0.9.1 connections for the node", ""},
    {"list_queues",             "Lists queues and their properties", ""},
    {"purge_queue",             "Purges a queue (removes all messages in it)", "<queue>"},
    {"pause_queue",             "Pause all consumers on a queue", "<queue>"},
    {"resume_queue",            "Resume all consumers on a queue", "<queue>"},
    {"export_definitions",      "Exports definitions in JSON", ""},
    {"import_definitions",      "Import definitions in JSON", "<file>"},
    {"close_all_connections",   "Instructs the broker to close all connections for the specified vhost or entire node", "<reason>"},
    {"close_connection",        "Instructs the broker to close a connection by pid", "<pid> <reason>"},
    {"stop_app",                "Stop the AMQP broker", ""},
    {"start_app",               "Starts the AMQP broker", ""},
    # {auth_user,               "Attempts to authenticate a user. Exits with a non-zero code if authentication fails.", "<username> <password>"},
    # {"clear_permissions",     "Revokes user permissions for a vhost", ""},
    # {"list_permissions",      "Lists user permissions in a virtual host", ""},
    # {"list_user_permissions", "Lists permissions of a user across all virtual hosts", ""},
    # {"set_permissions",       "Sets user permissions for a vhost", ""},
    # {"list_bindings",         "Lists all bindings on a vhost", ""},
    # {"list_channels",         "Lists all channels in the node", ""},
    # {"list_ciphers",          "Lists cipher suites supported by encoding commands", ""},
    # {"list_consumers",        "Lists all consumers for a vhost", ""},
    # {"list_exchanges",        "Lists exchanges", ""},
    # {"list_hashes",           "Lists hash functions supported by encoding commands", ""},
    # {"clear_parameter",       "Clears a runtime parameter.", ""},
    # {"list_parameters",       "Lists runtime parameters for a virtual host", ""},
    # {"set_parameter",         "Sets a runtime parameter.", ""},
  }


  def initialize
    self.banner = "Usage: #{PROGRAM_NAME} [arguments] entity"
    global_options
    @parser.on(CREATE, "Create entity (#{ENTITIES.join(", ")}), data is json") do
      @cmd = CREATE
      self.banner = "Usage: #{PROGRAM_NAME} #{CREATE} <data.json>"
    end
    @parser.on(REMOVE, "Remove entity (#{ENTITIES.join(", ")})") do
      @cmd = REMOVE
      self.banner = "Usage: #{PROGRAM_NAME} #{REMOVE} <entity> <name>"
    end
    @parser.on(LIST, "List entity (#{ENTITIES.join(", ")})") do
      @cmd = LIST
      self.banner = "Usage: #{PROGRAM_NAME} #{LIST} <entity> [name]"
    end
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
    @parser.on("-v", "--version", "Show version") { puts AvalancheMQ::VERSION; exit 0 }
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
    when REMOVE then remove
    when CREATE then create
    when LIST then list
    when "import_definitions" then import_definitions
    when "export_definitions" then export_definitions
    when "set_user_tags" then set_user_tags
    when "add_user" then add_user
    when "list_users" then list_users
    when "delete_user" then delete_user
    when "change_password" then change_password
    when "list_queues" then list_queues
    when "purge_queue" then purge_queue
    when "list_vhosts" then list_vhosts
    when "add_vhost" then add_vhost
    when "delete_vhost" then delete_vhost
    when "clear_policy" then clear_policy
    when "list_policies" then list_policies
    when "set_policy" then set_policy
    when "list_connections" then list_connections
    when "close_connection" then close_connection
    when "close_all_connections" then close_all_connections
    when "stop_app" then
    when "start_app" then
    else
      puts @parser
      abort
    end
  rescue ex : IO::Error
    abort ex
  ensure
    @http.try(&.close)
    @socket.try(&.close)
  end

  private def connect
    if host = @options["host"]?
      uri = URI.parse(host)
      HTTP::Client.new(uri.host.not_nil!, uri.port.not_nil!)
    else
      socket = UNIXSocket.new(AvalancheMQ::HTTP::INTERNAL_UNIX_SOCKET)
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

  private def handle_response(resp, ok = 200)
    if resp.status_code == ok
      exit 0
    else
      case resp.status_code
      when 404
        puts "Not found"
      else
        puts "Command failed: #{resp.body}"
      end
      exit 1
    end
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
    handle_response(resp)
  end

  private def export_definitions
    resp = http.get "/api/definitions", @headers
    if resp.status_code == 200
      print resp.body
    else
      handle_response(resp)
    end
  end

  private def list_users
    resp = http.get "/api/users", @headers
    return resp.body.to_s unless resp.status_code == 200
    puts "Listing users ..."
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
    resp = http.put "/api/users/#{username}", @headers, { password: password }.to_json
    handle_response(resp, 204)
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
    resp = http.put "/api/users/#{username}", @headers, { tags: tags }.to_json
    handle_response(resp, 204)
  end

  private def change_password
    username = ARGV.shift?
    pwd = ARGV.shift?
    abort @banner unless username && pwd
    resp = http.put "/api/users/#{username}", @headers, { password: pwd }.to_json
    handle_response(resp, 204)
  end

  private def list_queues
    vhost = @options["vhost"]? || "/"
    resp = http.get "/api/queues/#{URI.encode_www_form(vhost)}", @headers
    puts "Listing queues for vhost #{vhost} ..."
    return resp.body.to_s unless resp.status_code == 200
    return unless queues = JSON.parse(resp.body).as_a?
    puts "name\tmessages"
    queues.each do |u|
      next unless q = u.as_h?
      puts "#{q["name"]}\t#{q["messages"]}"
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
    columns =  ARGV
    columns = ["user", "peer_host", "peer_port", "state"] if columns.empty?
    resp = http.get "/api/connections", @headers
    puts "Listing connections ..." unless quiet?
    return unless resp.status_code == 200
    return unless conns = JSON.parse(resp.body).as_a?
    puts columns.join("\t")
    conns.each do |u|
      next unless conn = u.as_h?
      puts columns.map { |c| conn[c] }.join("\t")
    end
  end

  private def close_connection
    name = ARGV.shift?
    abort @banner unless name
    puts "Closing connection #{name} ..." unless quiet?
    @headers["X-Reason"] = ARGV.shift? || "CONNECTION_FORCED - Closed via avalanchemqctl"
    resp = http.delete "/api/connections/#{URI.encode(name)}", @headers
    handle_response(resp, 204)
  end

  private def close_all_connections
    resp = http.get "/api/connections", @headers
    return unless resp.status_code == 200
    return unless conns = JSON.parse(resp.body).as_a?
    @headers["X-Reason"] = ARGV.shift? || "CONNECTION_FORCED - Closed via avalanchemqctl"
    conns.each do |u|
      next unless conn = u.as_h?
      name = conn["name"].to_s
      puts "Closing connection #{name} ..." unless quiet?
      http.delete "/api/connections/#{URI.encode(name)}", @headers
    end
  end

  private def list_vhosts
    resp = http.get "/api/vhosts", @headers
    puts "Listing vhosts ..." unless quiet?
    return resp.body.to_s unless resp.status_code == 200
    return unless vhosts = JSON.parse(resp.body).as_a?
    puts "name"
    vhosts.each do |u|
      next unless v = u.as_h?
      puts "#{v["name"]}"
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
    puts "Listing policies for vhost #{vhost} ..."
    return resp.body.to_s unless resp.status_code == 200
    return unless policies = JSON.parse(resp.body).as_a?
    puts "vhost\tname\tpattern\tapply-to\tdefinition\tpriority"
    policies.each do |u|
      next unless p = u.as_h?
      puts "#{p["vhost"]}\t#{p["name"]}\t#{p["pattern"]}\t#{p["apply-to"]}\t#{p["definition"]}\t#{p["priority"]}"
    end
  end

  private def set_policy
    vhost = @options["vhost"]? || "/"
    name = ARGV.shift?
    pattern = ARGV.shift?
    definition = ARGV.shift?
    abort @banner unless name && pattern && definition
    body = {
      pattern: pattern,
      definition: JSON.parse(definition),
      "apply-to": @options["apply-to"]? || "all",
      "priority": @options["priority"]? || 0,
    }
    resp = http.put "/api/policies/#{URI.encode_www_form(vhost)}/#{name}", @headers, body.to_json
    handle_response(resp, 204)
  end

  private def remove
    name = ARGV.shift?
    abort @banner unless name
    entity = entity_arg
    resp = http.delete "/api/#{entity}", @headers,
      {name: name.to_s, vhost: @options["vhost"]?}.to_json
    handle_response(resp)
  end

  private def create
    name = ARGV.shift?
    abort @banner unless name
    entity = entity_arg
    resp = http.post "/api/#{entity}", @headers,
      {name: name.to_s, vhost: @options["vhost"]?}.to_json
    handle_response(resp)
  end

  private def list
    name = ARGV.size == 2 ? ARGV.shift : nil
    entity = entity_arg
    url = "/api/#{entity}"
    url += "/#{url_encoded_vhost}" if @options["vhost"]?
    url += "/#{name}" if name
    resp = http.get url
    puts "GET #{url} status=#{resp.status}"
    unless resp.status_code == 200
      puts resp.body
      exit 1
    end
    JSON.parse(resp.body).to_pretty_json(STDOUT)
    exit 0
  end
end

cli = AvalancheMQCtl.new
cli.run_cmd
