require "logger"

module AvalancheMQ
  class Config
    def initialize(
      @data_dir = "",
      @log_level : Logger::Severity = Logger::INFO,
      @bind = "::",
      @port = 5672,
      @tls_port = 5671,
      @unix_path = "",
      @cert_path = "",
      @key_path = "",
      @mgmt_bind = "::",
      @mgmt_port = 15672,
      @mgmt_tls_port = 15671,
      @mgmt_cert_path = "",
      @mgmt_key_path = "",
      @heartbeat = 60_u16,
      @segment_size : Int32 = 32 * 1024**2,
      @gc_segments_interval = 60,
      @queue_max_acks = 1_000_000,
      @stats_interval = 5000,
      @stats_log_size = 120, # 10 mins at 5s interval
      @set_timestamp = false,
      @file_buffer_size = 262_144,
      @socket_buffer_size = 8192
    )
      @@instance = self
    end

    property data_dir, log_level, bind, port, tls_port, unix_path,
      cert_path, key_path, mgmt_bind, mgmt_port,
      mgmt_tls_port, mgmt_cert_path, mgmt_key_path, heartbeat,
      gc_segments_interval, segment_size, queue_max_acks, stats_interval,
      stats_log_size, set_timestamp, file_buffer_size, socket_buffer_size

    def self.instance(*args)
      @@instance || new(*args)
    end

    def parse(file)
      return if file.empty?
      abort "Config could not be found" unless File.file?(file)
      ini = INI.parse(File.read(file))
      ini.each do |section, settings|
        case section
        when "main"
          parse_main(settings)
        when "amqp"
          parse_amqp(settings)
        when "mgmt"
          parse_mgmt(settings)
        end
      end
    end

    private def parse_main(settings)
      @data_dir = settings["data_dir"]? || @data_dir
      @log_level = Logger::Severity.parse?(settings["log_level"]?.to_s) || @log_level
      @stats_interval = settings["stats_interval"]?.try &.to_i32 || @stats_interval
      @stats_log_size = settings["stats_log_size"]?.try &.to_i32 || @stats_log_size
      @gc_segments_interval = settings["gc_segments_interval"]?.try &.to_i32 || @gc_segments_interval
      @segment_size = settings["segment_size"]?.try &.to_i32 || @segment_size
      @queue_max_acks = settings["queue_max_acks"]?.try &.to_i32 || @queue_max_acks
      @set_timestamp = true?(settings["set_timestamp"]?) || @set_timestamp
      @file_buffer_size = settings["file_buffer_size"]?.try &.to_i32 || @file_buffer_size
      @socket_buffer_size = settings["socket_buffer_size"]?.try &.to_i32 || @socket_buffer_size
    end

    private def parse_amqp(settings)
      @bind = settings["bind"]? || @bind
      @port = settings["port"]?.try &.to_i32 || @port
      @tls_port = settings["tls_port"]?.try &.to_i32 || @tls_port
      @unix_path = settings["unix_path"]? || @unix_path
      @cert_path = settings["tls_cert"]? || @cert_path
      @key_path = settings["tls_key"]? || @key_path
      @heartbeat = settings["heartbeat"]?.try &.to_u16 || @heartbeat
    end

    private def parse_mgmt(settings)
      @mgmt_bind = settings["bind"]? || @mgmt_bind
      @mgmt_port = settings["port"]?.try &.to_i32 || @mgmt_port
      @mgmt_tls_port = settings["tls_port"]?.try &.to_i32 || @mgmt_tls_port
      @mgmt_cert_path = settings["tls_cert"]? || @mgmt_cert_path
      @mgmt_key_path = settings["tls_key"]? || @mgmt_key_path
    end

    private def true?(str : String?)
      %w(true yes y 1).includes? str
    end
  end
end
