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

    # List all segments in the S3 bucket for this stream
    # Returns a hash mapping segment ID to segment info
    def s3_segments_from_bucket(max_retries = 5) : Hash(UInt32, NamedTuple(path: String, etag: String, size: Int64, meta: Bool))
      s3_segments = Hash(UInt32, NamedTuple(path: String, etag: String, size: Int64, meta: Bool)).new
      prefix = @msg_dir[Config.instance.data_dir.bytesize + 1..] + "/"
      continuation_token : String? = nil
      retries = 0

      loop do
        begin
          path = "/?delimiter=%2F&encoding-type=url&list-type=2&prefix=#{prefix}&max-keys=1000"
          if token = continuation_token
            path += "&continuation-token=#{URI.encode_path(token)}"
          end
          response = http_client(with_timeouts: true).get(path)
          continuation_token = list_of_files_from_xml(XML.parse(response.body), s3_segments)
          retries = 0 # Reset on success
          break unless continuation_token
        rescue ex : IO::TimeoutError | IO::Error
          retries += 1
          if retries > max_retries
            @log.error { "Failed to list S3 bucket after #{max_retries} retries: #{ex.message}" }
            break
          end
          @log.warn { "Error listing S3 bucket (attempt #{retries}/#{max_retries}): #{ex.message}" }
          sleep (2 ** retries).milliseconds # Exponential backoff
        end
      end

      @log.info { "Found #{s3_segments.size} segments in S3" }
      s3_segments
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

    # Download a segment from S3
    # Returns MFile on success, nil on failure
    # Does NOT retry - caller is responsible for retry logic
    def download_segment(segment_id : UInt32, s3_segments, h : ::HTTP::Client) : MFile?
      @log.debug { "Downloading segment: #{segment_id}" }
      return unless s3_segments[segment_id]?

      s3file_path = s3_segments[segment_id][:path]
      path = File.join(Config.instance.data_dir, s3file_path)
      temp_path = make_temp_path(path)

      h.get("/#{s3file_path}") do |response|
        if response.status_code != 200
          @log.warn { "Failed to download segment #{segment_id}: HTTP #{response.status_code}" }
          return nil
        end

        bytesize = response.headers["Content-Length"].to_i32
        rfile = MFile.new(temp_path, bytesize)

        begin
          IO.copy response.body_io, rfile

          # Check if another download completed while we were downloading
          if File.exists?(path)
            @log.debug { "Segment #{segment_id} already exists, discarding download" }
            rfile.delete
            rfile.close
            return nil
          end

          rfile.rename(path)
          @log.debug { "Downloaded segment: #{segment_id}" }
          return rfile
        rescue ex
          # Clean up temp file on any error
          rfile.delete rescue nil
          rfile.close rescue nil
          raise ex
        end
      end
    end

    # Download metadata file from S3
    # Returns MFile on success, nil on failure
    # Does NOT retry - caller is responsible for retry logic
    def download_meta_file(segment_id : UInt32, s3_segments, h : ::HTTP::Client) : MFile?
      @log.debug { "Downloading meta for segment: #{segment_id}" }
      return unless s3_segments[segment_id]?

      s3_meta_path = meta_file_name(s3_segments[segment_id][:path])
      path = File.join(Config.instance.data_dir, s3_meta_path)

      h.get("/#{s3_meta_path}") do |response|
        if response.status_code != 200
          @log.debug { "Meta file not found for segment #{segment_id}: HTTP #{response.status_code}" }
          return nil
        end
        bytesize = response.headers["Content-Length"].to_i32
        rfile = MFile.new(path, bytesize)
        IO.copy response.body_io, rfile
        return rfile
      end
    end

    # Upload a file to S3
    # Retries on failure with exponential backoff
    def upload_file_to_s3(path : String, slice : Bytes, max_retries = 3) : String
      retries = 0
      loop do
        begin
          response = http_client(with_timeouts: true).put(path, body: slice)
          if response.status_code == 200
            return response.headers["ETag"]
          end
          raise "HTTP #{response.status_code}"
        rescue ex
          retries += 1
          if retries > max_retries
            raise Exception.new("Failed to upload #{path} to S3 after #{max_retries} retries: #{ex.message}")
          end
          @log.warn { "Failed to upload #{path} to S3 (attempt #{retries}/#{max_retries}): #{ex.message}" }
          sleep (2 ** retries).milliseconds
        end
      end
    end

    # Delete a segment and its metadata from S3
    def delete_from_s3(s3_seg)
      h = http_client(with_timeouts: true)
      begin
        delete_from_s3(h, s3_seg[:path])
        delete_from_s3(h, meta_file_name(s3_seg[:path]))
      ensure
        h.close
      end
    end

    def delete_from_s3(h : ::HTTP::Client, path : String)
      response = h.delete("/#{path}")
      if response.status_code != 204
        @log.error { "Failed to delete #{path} from S3: HTTP #{response.status_code}" }
      else
        @log.debug { "Deleted #{path} from S3" }
      end
    end

    # Create an HTTP client for S3 operations
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

    private def make_temp_path(path) : String
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
