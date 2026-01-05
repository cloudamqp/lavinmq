require "json"
require "wait_group"
require "../../server"

module LavinMQ
  class EntitySearch
    Log = LavinMQ::Log.for("entity_search")

    def initialize(@server : Server, @ws : ::HTTP::WebSocket)
      Log.info { "Opened" }
    end

    def search
      @ws.on_message do |message|
        Log.debug { "on_message: #{message}" }
        @ws.stream do |io|
          JSON.build(io) do |json|
            json.object do
              json.field "query", message
              json.field "result" do
                json.array do
                  search_vhosts(json, message)
                end
              end
            end
          end
        end
        Log.debug { "Response sent..?" }
      end
      @ws.on_close do
        Log.info { "closed" }
      end
      @ws.run
      Log.info { "search done" }
    end

    MAX_RESULTS = 10

    private def search_vhosts(json : JSON::Builder, query : String)
      # If the query begins with any "type" followed by a space, we'll
      # only search for that type, else we search for everything
      types = %w{queue exchange vhost user}
      search_types = types.find do |type|
        next unless query.starts_with?(type)
        next unless query.size > (type.size + 1)
        next unless query[type.size] == ' '
        query = query[(type.size + 1)..]
        {type}
      end || types
      Log.debug { "search_types = #{search_types} query=#{query}" }

      count = 0
      if %w{queue exchange vhost}.any? &.in?(search_types)
        @server.vhosts.each_value do |vhost|
          if "queue".in?(search_types)
            vhost.queues.each_value do |queue|
              if queue.search_match?(query)
                count += 1
                {queue: queue.name, vhost: vhost.name}.to_json(json)
              end
              return if count >= MAX_RESULTS
            end
          end

          if "exchange".in?(search_types)
            vhost.exchanges.each_value do |exchange|
              if exchange.search_match?(query)
                count += 1
                {exchange: exchange.name, vhost: vhost.name}.to_json(json)
              end
              return if count >= MAX_RESULTS
            end
          end

          if "vhost".in?(search_types)
            if vhost.search_match?(query)
              count += 1
              {vhost: vhost.name}.to_json(json)
            end
            return if count >= MAX_RESULTS
          end
        end
      end
      if "user".in?(search_types)
        @server.users.each_value do |user|
          if user.search_match?(query)
            count += 1
            {user: user.name}.to_json(json)
          end
          return if count >= MAX_RESULTS
        end
      end
    end

    private macro stream_entities(json, field_name, counter, entities)
      return unless {{counter}} > 0 
      {{json}}.field {{field_name}} do
        json.array do
          {{counter}}.times do |i|
            {{entities}}[i].to_json({{json}})
          end
        end
      end
    end
  end

  # Acts as a proxy between websocket clients and the normal TCP servers
  class WebsocketProxy
    def self.new(server : Server)
      ::HTTP::WebSocketHandler.new do |ws, ctx|
        req = ctx.request
        local_address = req.local_address.as?(Socket::IPAddress) ||
                        Socket::IPAddress.new("127.0.0.1", 0) # Fake when UNIXAddress
        remote_address = req.remote_address.as?(Socket::IPAddress) ||
                         Socket::IPAddress.new("127.0.0.1", 0) # Fake when UNIXAddress
        connection_info = ConnectionInfo.new(remote_address, local_address)
        io = WebSocketIO.new(ws)
        case req.path
        when "/mqtt", "/ws/mqtt"
          spawn server.handle_connection(WebSocketIO.new(ws), connection_info, Server::Protocol::MQTT), name: "HandleWSconnection MQTT #{remote_address}"
        when "/api/entity-search"
          spawn EntitySearch.new(server, ws).search
        else
          spawn server.handle_connection(WebSocketIO.new(ws), connection_info, Server::Protocol::AMQP), name: "HandleWSconnection AMQP #{remote_address}"
        end
      end
    end
  end

  class WebSocketIO < IO
    include IO::Buffered

    def initialize(@ws : ::HTTP::WebSocket)
      @r, @w = IO.pipe
      @r.read_buffering = false
      @w.sync = true
      @ws.on_binary do |slice|
        @w.write(slice)
      end
      @ws.on_close do |_code, _message|
        close
      end
      self.buffer_size = 4096
    end

    def unbuffered_read(slice : Bytes)
      @r.read(slice)
    end

    def unbuffered_write(slice : Bytes) : Nil
      @ws.send(slice)
    end

    def unbuffered_flush
    end

    def unbuffered_rewind
    end

    def unbuffered_close
      return if @closed
      @closed = true
      @r.close
      @w.close
      @ws.close
    end

    def read_timeout=(timeout : Time::Span?)
      @r.read_timeout = timeout
    end
  end
end
