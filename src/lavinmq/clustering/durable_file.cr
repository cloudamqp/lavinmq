module LavinMQ
  module Clustering
    module DurableFile
      def self.replace(path : String, perm : Int32 = 0o600, & : File -> Nil) : Nil
        dir = File.dirname(path)
        Dir.mkdir_p(dir)

        tmp_path = nil
        File.tempfile("#{File.basename(path)}.", ".tmp", dir: dir) do |io|
          tmp_path = io.path
          io.chmod(perm)
          yield io
          io.fsync
        end
        if tmp = tmp_path
          File.rename(tmp, path)
          sync_dir(dir)
        else
          raise "Temporary file was not created for #{path}"
        end
      ensure
        if tmp = tmp_path
          File.delete?(tmp)
        end
      end

      private def self.sync_dir(dir : String) : Nil
        {% if flag?(:unix) %}
          File.open(dir) do |io|
            io.fsync
          end
        {% end %}
      end
    end
  end
end
