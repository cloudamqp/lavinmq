module AvalancheMQ
  class SegmentPositionMigrator
    @file_name = "segment_position_version.txt"
    @current_version : UInt32
    def initialize(@data_dir : String, @log : Logger, @format = IO::ByteFormat::SystemEndian,
                   version : UInt32? = nil)

      if !version.nil?
        @current_version = version
      elsif ver = read_version_from_disk
        @current_version = ver
      else
        write_version_to_disk(SegmentPosition::VERSION)
        @current_version = SegmentPosition::VERSION
      end

      @log.progname = "sp_migrator"
    end

    def read_version_from_disk
      path = File.join(@data_dir, @file_name)
      if File.exists? path
        @log.debug "Reading SP version from disk."
        File.read(path).to_u32?
      else
        nil
      end
    end

    def write_version_to_disk(version)
      path = File.join(@data_dir, @file_name)
      Dir.mkdir_p @data_dir
      File.write(path, version)
    end

    def run(sp_file_path)
      target_version = SegmentPosition::VERSION
      return if @current_version == target_version
      convert_sp(target_version, SegmentPosition::SP_FORMATS, sp_file_path)
      write_version_to_disk(target_version)
    end

    def convert_sp(target_version, sp_formats, sp_file_path)
      tmp_file = sp_file_path + ".tmp"
      current_format = sp_formats[@current_version]
      target_format = sp_formats[target_version]
      File.open(tmp_file, "w") do |tmp_io|
        File.open(sp_file_path) do |sp_io|
          loop do
            target_index = 0
            current_format.each_with_index do |data_type, i|
              sp_part = data_type.class.from_io(sp_io, @format)
              if target_data_type = target_format.fetch(i, nil)
                target_part = sp_part.as(typeof(target_data_type))
                tmp_io.write_bytes(target_part, @format)
              end
              target_index = i
            end
            if target_format.size > current_format.size
              target_format[(target_index + 1)...].each do |tf|
                tmp_io.write_bytes(tf, @format)
              end
            end
          rescue e: IO::EOFError
            break
          end
        end
      end
      File.rename(tmp_file, sp_file_path)
    end
  end
end
