require "openssl"

lib LibSSL
  {% unless LibSSL.has_method?(:ssl_has_pending) %}
    fun ssl_has_pending = SSL_has_pending(handle : SSL) : Int
  {% end %}
  {% unless LibSSL.has_method?(:ssl_set_read_ahead) %}
    fun ssl_set_read_ahead = SSL_set_read_ahead(handle : SSL, yes : Int)
  {% end %}
end

module OffloadedSSL
  module Socket
    class Server < OpenSSL::SSL::Socket::Server
      getter execution_context : Fiber::ExecutionContext
      @read_requests : Channel(Bytes)
      @read_responses : Channel({Int32, Exception?})
      @write_requests : Channel(Bytes)
      @write_responses : Channel(Exception?)

      def initialize(io, context : OpenSSL::SSL::Context, @execution_context : Fiber::ExecutionContext, sync_close : Bool = true)
        super(io, context, sync_close: sync_close)
        @read_requests = Channel(Bytes).new
        @read_responses = Channel({Int32, Exception?}).new
        @write_requests = Channel(Bytes).new
        @write_responses = Channel(Exception?).new

        fiber_identifier = if io.responds_to?(:remote_address)
                             io.remote_address.to_s
                           else
                             io.object_id.to_s
                           end

        # Maybe not necessary?
        # LibSSL.ssl_set_read_ahead(@ssl, 1)

        @execution_context.spawn(name: "#{fiber_identifier}:reader") do
          loop do
            begin
              ex : Exception? = nil
              slice = @read_requests.receive
              check_open
              if slice.size == 0
                @read_responses.send({0, ex})
                next
              end
              bytes_read = drain_read(slice)
              @read_responses.send({bytes_read, ex})
            rescue ex : ::Channel::ClosedError
              break
            rescue ex
              @read_responses.send({0, ex})
            end
          end
        end

        @execution_context.spawn(name: "#{fiber_identifier}:writer") do
          loop do
            begin
              slice = @write_requests.receive
              check_open
              # Maybe bad to assume flush?
              if slice.empty?
                @bio.io.flush
                @write_responses.send(nil)
                next
              end
              count = slice.size
              bytes = LibSSL.ssl_write(@ssl, slice.to_unsafe, count)
              unless bytes > 0
                raise OpenSSL::SSL::Error.new(@ssl, bytes, "SSL_write")
              end
              @write_responses.send(nil)
            rescue ex : ::Channel::ClosedError
              break
            rescue ex
              @write_responses.send(ex)
            end
          end
        end
      end

      private def drain_read(slice) : Int32
        bytes_read = 0
        loop do
          break if slice.size == 0
          bytes = LibSSL.ssl_read(@ssl, slice.to_unsafe, slice.size)
          if bytes <= 0
            error = LibSSL.ssl_get_error(@ssl, bytes)
            # Handle "want read" and "want write"?
            if !(error.zero_return?)
              ex = OpenSSL::SSL::Error.new(@ssl, bytes, "SSL_read")
              if ex.underlying_eof?
                # underlying BIO terminated gracefully, without terminating SSL aspect gracefully first
                # some misbehaving servers "do this" so treat as EOF even though it's a protocol error
                break
              end
              raise ex
            end
          else
            bytes_read += bytes
            slice += bytes
          end
          pending = LibSSL.ssl_has_pending(@ssl)
          if bytes == 0 || pending == 0
            break
          end
        end
        bytes_read
      end

      def unbuffered_read(slice : Bytes) : Int32
        check_open
        return 0 if slice.size == 0
        @read_requests.send(slice)
        bytes_read, ex = @read_responses.receive
        raise ex if ex
        bytes_read
      end

      def unbuffered_write(slice : Bytes) : Nil
        check_open
        return if slice.empty?
        @write_requests.send(slice)
        ex = @write_responses.receive
        raise ex if ex
      end

      def unbuffered_flush : Nil
        # Is this enough? hm
        unbuffered_write(Bytes.empty)
      end

      def unbuffered_close : Nil
        @read_requests.close
        @write_requests.close
        @read_responses.close
        @write_responses.close
        super
      end
    end
  end
end
