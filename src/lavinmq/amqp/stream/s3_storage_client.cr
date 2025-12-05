require "awscr-signer"
require "http/client"
require "xml"
require "../../config"
require "../../rough_time"
require "../../mfile"

module LavinMQ::AMQP
  class S3StorageClient
    @s3_signer : Awscr::Signer::Signers::V4
    @log : Logger

    HTTP_CONNECT_TIMEOUT = 200.milliseconds
    HTTP_READ_TIMEOUT    = 500.milliseconds

    def initialize(@msg_dir : String, metadata : ::Log::Metadata = ::Log::Metadata.empty)
      @log = Logger.new(Log, metadata)
      @s3_signer = s3_signer
    end

    def s3_segments_from_bucket(retries = 5)
      s3_segments = Hash(UInt32, NamedTuple(path: String, etag: String, size: Int64, meta: Bool)).new
      prefix = @msg_dir[Config.instance.data_dir.bytesize + 1..] + "/"
      continuation_token = ""

      loop do
        path = "?delimiter=%2F&encoding-type=url&list-type=2&prefix=#{prefix}&max-keys=1000"
        path += "&continuation-token=#{URI.encode_path(continuation_token)}" unless continuation_token.empty?
        response = http_client.get(path)
        continuation_token = list_of_files_from_xml(XML.parse(response.body), s3_segments)
        break unless continuation_token
      end

      @log.info { "Found #{s3_segments.size} segments in S3" }
      s3_segments
    rescue ex : IO::TimeoutError
      @log.error { "Timeout while downloading file list, retrying..." }
      if retries > 0
        s3_segments_from_bucket(retries - 1)
      else
        s3_segments
      end
    end

    def list_of_files_from_xml(document, s3_segments) : String?
      list_bucket_results = document.first_element_child
      return unless list_bucket_results

      contents_elements = list_bucket_results.xpath_nodes("//*[local-name()='Contents']")
      contents_elements.each { |content| parse_xml_element(content, s3_segments) }

      is_truncated_node = list_bucket_results.xpath_node(".//*[local-name()='IsTruncated']")
      if is_truncated_node && is_truncated_node.content == "true"
        continuation_token_node = list_bucket_results.xpath_node(".//*[local-name()='NextContinuationToken']")
        return continuation_token_node.try &.content
      end
      nil
    end

    private def parse_xml_element(content, s3_segments)
      path = etag = ""
      id = 0_u32
      size = 0_i64

      if key_node = content.xpath_node(".//*[local-name()='Key']")
        path = key_node.content
        if match = path.match(/\/meta\.(\d{10})$/)
          id = match[1].to_u32
          update_s3_segment_list(s3_segments, id, "", "", 0_i64, true)
          return
        elsif match = path.match(/\/msgs\.(\d{10})$/)
          id = match[1].to_u32
        else
          return
        end
      end

      if etag_node = content.xpath_node(".//*[local-name()='ETag']")
        etag_content = etag_node.content
        if etag_content.starts_with?('"')
          etag = etag_content[1..-2]
        else
          etag = etag_content
        end
      end

      if size_node = content.xpath_node(".//*[local-name()='Size']")
        size = size_node.content.to_i64
      end

      update_s3_segment_list(s3_segments, id, path, etag, size, false)
    end

    private def update_s3_segment_list(s3_segments, seg_id : UInt32, path : String = "", etag : String = "", size : Int64 = 0_i64, meta : Bool = false)
      s3_seg = s3_segments[seg_id]? || {path: path, etag: etag, size: size, meta: meta}
      path = s3_seg[:path] if path == ""
      etag = s3_seg[:etag] if etag == ""
      size = s3_seg[:size] if size == 0_i64
      meta = s3_seg[:meta] if meta == false

      s3_segments[seg_id] = {path: path, etag: etag, size: size, meta: meta}
    end

    def download_segment(segment_id : UInt32, s3_segments, h = http_client) : MFile?
      @log.debug { "Downloading segment: #{segment_id}" }
      return unless s3_segments[segment_id]?

      s3file_path = s3_segments[segment_id][:path]
      path = File.join(Config.instance.data_dir, s3file_path)
      temp_path = temp_path(path)

      h.get("/#{s3file_path}") do |response|
        bytesize = response.headers["Content-Length"].to_i32
        rfile = MFile.new(temp_path, bytesize)
        IO.copy response.body_io, rfile
        if File.exists?(path)
          rfile.delete
          return
        else
          rfile.rename(path)
          @log.debug { "Downloaded segment: #{segment_id}" }
          return rfile
        end
      rescue ex : IO::Error
        File.delete(temp_path) if File.exists?(temp_path)
        raise ex
      end
    rescue ex : IO::TimeoutError
      @log.warn { "Timeout while downloading segment #{segment_id}, retrying" }
      download_segment(segment_id, s3_segments, h)
    end

    def download_meta_file(segment_id : UInt32, s3_segments, h = http_client) : MFile?
      @log.debug { "Downloading meta for segment: #{segment_id}" }
      return unless s3_segments[segment_id]?

      s3_meta_path = meta_file_name(s3_segments[segment_id][:path])
      path = File.join(Config.instance.data_dir, s3_meta_path)

      h.get("/#{s3_meta_path}") do |response|
        if response.status_code != 200
          @log.error { "Failed to download meta for segment #{segment_id}, status: #{response.status_code}" }
          return nil
        end
        bytesize = response.headers["Content-Length"].to_i32
        rfile = MFile.new(path, bytesize)
        IO.copy response.body_io, rfile
        return rfile
      end
    rescue ex : IO::TimeoutError
      @log.warn { "Timeout while downloading meta for segment #{segment_id}, retrying" }
      download_meta_file(segment_id, s3_segments, h)
    end

    def upload_file_to_s3(path, slice, retries = 3) : String
      response = http_client.put(path, body: slice)
      if response.status_code != 200
        if (retries -= 1) <= 0
          raise Exception.new("Failed to upload file #{path} to S3 after multiple retries")
        end
        @log.warn { "Failed to upload file #{path} to S3, retrying" }
        upload_file_to_s3(path, slice, retries)
      end
      response.headers["ETag"]
    end

    def delete_from_s3(s3_seg)
      h = http_client
      delete_from_s3(h, s3_seg[:path])
      delete_from_s3(h, meta_file_name(s3_seg[:path]))
    end

    def delete_from_s3(h : ::HTTP::Client, path : String)
      response = h.delete("/#{path}")
      if response.status_code != 204
        @log.error { "Failed to delete #{path} from S3, status: #{response.status_code}" }
      else
        @log.debug { "Deleted #{path} from S3" }
      end
    end

    # with_timeouts doesn't seem to be used anywhere? we should add that where needed
    def http_client(with_timeouts = false) : ::HTTP::Client
      endpoint = Config.instance.streams_s3_storage_endpoint
      raise "S3 storage endpoint not configured" unless endpoint

      h = ::HTTP::Client.new(URI.parse(endpoint))
      h.before_request do |request|
        @s3_signer.sign(request)
      end
      if with_timeouts
        h.connect_timeout = HTTP_CONNECT_TIMEOUT
        h.read_timeout = HTTP_READ_TIMEOUT
      end
      h
    end

    def s3_signer : Awscr::Signer::Signers::V4
      if (region = Config.instance.streams_s3_storage_region) &&
         (access_key = Config.instance.streams_s3_storage_access_key_id) &&
         (secret_key = Config.instance.streams_s3_storage_secret_access_key)
        Awscr::Signer::Signers::V4.new("s3", region, access_key, secret_key)
      else
        Log.fatal { "S3 storage for streams is enabled, but region or access key is not set" }
        abort "S3 storage for streams is enabled, but region or access key is not set"
      end
    end

    private def temp_path(path)
      temp_path = "#{path}.tmp"
      i = 0
      while File.exists?(temp_path)
        temp_path = "#{path}.tmp.#{i}"
        i += 1
      end
      temp_path
    end

    private def meta_file_name(path : String) : String
      # We assume the path ends with "msgs.<10 chars>"
      raw = path.to_slice

      # This is basically the same as using sub("msgs.", "meta.") but with #sub
      # the first occurrence of "msgs." would be replaced, not the last one.
      # This also requires only one allocation.
      String.build(path.size) do |io|
        io.write raw[0, raw.size - 15]
        io.write "meta.".to_slice
        io.write raw[-10..]
      end
    end
  end
end
