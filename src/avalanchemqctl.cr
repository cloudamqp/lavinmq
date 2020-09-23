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
  IMPORT_DEF = "import_definitions"
  REMOVE     = "remove"
  CREATE     = "create"
  LIST       = "list"

  ENTITIES = ["queues", "exchanges", "vhosts", "policies"]

  def initialize
    self.banner = "Usage: #{PROGRAM_NAME} [arguments] entity"
    @parser.on(CREATE, "Create entity (#{ENTITIES.join(", ")}), data is json") do
      @cmd = CREATE
      self.banner = "Usage: #{PROGRAM_NAME} #{CREATE} <data.json>"
      global_options
    end
    @parser.on(REMOVE, "Remove entity (#{ENTITIES.join(", ")})") do
      @cmd = REMOVE
      self.banner = "Usage: #{PROGRAM_NAME} #{REMOVE} <entity> <name>"
      global_options
    end
    @parser.on(LIST, "List entity (#{ENTITIES.join(", ")})") do
      @cmd = LIST
      self.banner = "Usage: #{PROGRAM_NAME} #{LIST} <entity> [name]"
      global_options
    end
    @parser.on(IMPORT_DEF, "Imports definitions in JSON") do
      @cmd = IMPORT_DEF
      self.banner = "Usage: #{PROGRAM_NAME} #{IMPORT_DEF} <definitions.json>"
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

  def run_cmd
    @parser.parse
    case @cmd
    when IMPORT_DEF
      import_definitions
    when REMOVE
      remove
    when CREATE
      create
    when LIST
      list
    else
      abort "Unrecognised command #{@cmd}"
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
      puts resp.body
      exit 1
    end
  end

  private def url_encoded_vhost
    URI.encode_www_form(@options["vhost"])
  end

  private def import_definitions
    file = ARGV.shift?
    abort @banner unless file && File.exists?(file)
    data = File.read(file)
    resp = http.post "/api/definitions", @headers, data.to_s
    handle_response(resp)
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
