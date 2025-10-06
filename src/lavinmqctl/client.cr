require "http/client"
require "json"
require "uri"
require "../lavinmq/http/constants"

module LavinMQCtl
  class Client
    @http : HTTP::Client?
    @headers = HTTP::Headers{"Content-Type" => "application/json"}
    getter options

    def initialize(@options : Hash(String, String), @io : IO = STDOUT)
    end

    def connect
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

    def client_from_uri(uri : String)
      client_from_uri(URI.parse(uri))
    rescue ex : ArgumentError
      abort "Invalid URI. #{ex.message}"
    end

    def client_from_uri(uri : URI)
      c = HTTP::Client.new(uri)
      uri.user = @options["user"] if @options["user"]?
      uri.password = @options["password"] if @options["password"]?
      c.basic_auth(uri.user, uri.password) if uri.user
      c
    end

    def validate_connection_args(input_arg : String)
      invalid_args = Array(String).new
      invalid_args << "hostname" if @options["hostname"]?
      invalid_args << "port" if @options["port"]?
      invalid_args << "scheme" if @options["scheme"]?
      abort "invalid args when using #{input_arg}: #{invalid_args.join(", ")}" unless invalid_args.empty?
    end

    def http
      @http ||= connect
    end

    def close
      @http.try(&.close)
    end

    def headers
      @headers
    end

    def handle_response(resp, *ok)
      return if ok.includes? resp.status_code
      if resp.status_code == 503
        output resp.body
        exit 2
      end
      output "#{resp.status_code} - #{resp.status}"
      output resp.body if resp.body? && !resp.headers["Content-Type"]?.try(&.starts_with?("text/html"))
      exit 1
    end

    def output(data, columns = nil)
      if @options["format"]? == "json"
        data.to_json(@io)
        puts
      else
        case data
        when Hash, NamedTuple
          data.each do |k, v|
            @io << k << ": " << v << "\n"
          end
        when Array
          output_array(data, columns)
        else
          puts data
        end
      end
    end

    def output_array(data : Array, columns : Array(String)?)
      if columns
        puts columns.join(@io, "\t")
      else
        case first = data.first?
        when NamedTuple
          puts first.keys.join(@io, "\t")
        when JSON::Any
          puts first.as_h.each_key.join(@io, "\t")
        end
      end
      data.each do |item|
        case item
        when Hash
          item.each_value.join(@io, "\t")
        when JSON::Any
          item.as_h.each_value.join(@io, "\t")
        when NamedTuple
          item.values.join(@io, "\t")
        else
          item.to_s(@io)
        end
        puts
      end
    end

    def get(url, page = 1, items = Array(JSON::Any).new)
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

    def print_erlang_terms(h : Hash)
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

    def quiet?
      @options["quiet"]? || @options["silent"]? || @options["format"]? == "json"
    end
  end
end
