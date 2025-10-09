require "option_parser"
require "json"
require "../lavinmq/version"

module LavinMQCtl
  class Parser
    @parser = OptionParser.new
    @banner : String = "Usage: #{PROGRAM_NAME} [arguments] entity"
    @cmd : String?
    getter options : Hash(String, String)
    getter args : Hash(String, JSON::Any)
    getter cmd
    getter banner

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
      @options = {} of String => String
      @args = {} of String => JSON::Any
      @cmd = nil

      if host = ENV["LAVINMQCTL_HOST"]?
        @options["host"] = host
      end

      @parser.banner = @banner
      setup_global_options
      setup_commands
    end

    def banner=(new_banner : String)
      @banner = new_banner
      @parser.banner = new_banner
    end

    def parse(args = ARGV)
      @parser.parse(args)
    rescue ex : OptionParser::MissingOption
      abort ex
    end

    def help
      puts @parser
      exit 1
    end

    private def setup_global_options
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

    private def setup_commands
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
          @options["internal"] = "true"
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
      @parser.on("-h", "--help", "Show this help") { help }
      @parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
    end
  end
end
