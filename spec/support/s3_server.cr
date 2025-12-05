require "http/server"
require "digest/md5"
require "xml"

# MinimalS3Server provides an in-memory S3-compatible server for testing
# Supports: ListObjectsV2, GetObject, PutObject, DeleteObject
class MinimalS3Server
  @storage = Hash(String, Bytes).new
  @server : HTTP::Server?
  @port : Int32

  def initialize(@port = 0)
  end

  def start
    server = HTTP::Server.new do |context|
      begin
        handle_request(context)
      rescue ex
        context.response.status = HTTP::Status::INTERNAL_SERVER_ERROR
        context.response.print "Internal server error: #{ex.message}"
      end
    end

    # Bind to localhost
    address = server.bind_tcp "127.0.0.1", @port
    @port = address.port
    @server = server

    # Start server in background
    spawn do
      server.listen
    end

    # Give server time to start
    sleep 100.milliseconds
  end

  def stop
    @server.try(&.close)
  end

  def port : Int32
    @port
  end

  def endpoint : String
    "127.0.0.1:#{@port}"
  end

  def clear
    @storage.clear
  end

  def put(key : String, data : Bytes)
    @storage[key] = data
  end

  def get(key : String) : Bytes?
    @storage[key]?
  end

  def delete(key : String)
    @storage.delete(key)
  end

  def keys : Array(String)
    @storage.keys
  end

  private def handle_request(context : HTTP::Server::Context)
    request = context.request
    response = context.response

    case request.method
    when "GET"
      handle_get(request, response)
    when "PUT"
      handle_put(request, response)
    when "DELETE"
      handle_delete(request, response)
    when "HEAD"
      handle_head(request, response)
    else
      response.status = HTTP::Status::METHOD_NOT_ALLOWED
      response.print "Method not allowed"
    end
  end

  private def handle_get(request : HTTP::Request, response : HTTP::Server::Response)
    query = request.query_params

    # ListObjectsV2
    if query["list-type"]? == "2"
      handle_list_objects(request, response, query)
    else
      # GetObject
      handle_get_object(request, response)
    end
  end

  private def handle_list_objects(request : HTTP::Request, response : HTTP::Server::Response, query : URI::Params)
    prefix = query["prefix"]? || ""

    # Filter keys by prefix
    matching_keys = @storage.keys.select(&.starts_with?(prefix))

    # Build XML response
    xml_resp = XML.build(indent: "  ") do |xml|
      xml.element("ListBucketResult", xmlns: "http://s3.amazonaws.com/doc/2006-03-01/") do
        matching_keys.each do |key|
          data = @storage[key]
          etag = calculate_etag(data)

          xml.element("Contents") do
            xml.element("Key") { xml.text key }
            xml.element("ETag") { xml.text %("#{etag}") }
            xml.element("Size") { xml.text data.size.to_s }
          end
        end
      end
    end

    response.status = HTTP::Status::OK
    response.headers["Content-Type"] = "application/xml"
    response.headers["Content-Length"] = xml_resp.bytesize.to_s
    response.print xml_resp
  end

  private def handle_get_object(request : HTTP::Request, response : HTTP::Server::Response)
    key = request.path.lstrip('/')

    if data = @storage[key]?
      etag = calculate_etag(data)
      response.status = HTTP::Status::OK
      response.headers["Content-Length"] = data.size.to_s
      response.headers["ETag"] = %("#{etag}")
      response.write(data)
    else
      response.status = HTTP::Status::NOT_FOUND
      response.print "Not Found"
    end
  end

  private def handle_put(request : HTTP::Request, response : HTTP::Server::Response)
    key = request.path.lstrip('/')

    # Read body
    body = Bytes.new(0)
    if content_length = request.headers["Content-Length"]?.try(&.to_i?)
      body = Bytes.new(content_length)
      request.body.try(&.read_fully(body))
    elsif request_body = request.body
      io = IO::Memory.new
      IO.copy(request_body, io)
      body = io.to_slice
    end

    @storage[key] = body
    etag = calculate_etag(body)

    response.status = HTTP::Status::OK
    response.headers["ETag"] = %("#{etag}")
    response.print ""
  end

  private def handle_delete(request : HTTP::Request, response : HTTP::Server::Response)
    key = request.path.lstrip('/')
    @storage.delete(key)

    response.status = HTTP::Status::NO_CONTENT
    response.print ""
  end

  private def handle_head(request : HTTP::Request, response : HTTP::Server::Response)
    key = request.path.lstrip('/')

    if data = @storage[key]?
      etag = calculate_etag(data)
      response.status = HTTP::Status::OK
      response.headers["Content-Length"] = data.size.to_s
      response.headers["ETag"] = %("#{etag}")
    else
      response.status = HTTP::Status::NOT_FOUND
    end
  end

  private def calculate_etag(data : Bytes) : String
    Digest::MD5.hexdigest(data)
  end
end
