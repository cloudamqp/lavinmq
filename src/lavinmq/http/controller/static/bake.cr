require "digest/md5"
require "compress/zlib"

def recursive_bake(dir)
  Dir.each_child(dir) do |child|
    path = File.join(dir, child)
    if File.directory? path
      recursive_bake path
    else
      if already_compressed?(path)
        data = File.read(path)
        etag = %(W/"#{Digest::MD5.hexdigest(data)}")
        deflated = false
      else
        io = IO::Memory.new
        Compress::Zlib::Writer.open(io) do |zlib|
          File.open(path) do |f|
            etag = %(W/"#{Digest::MD5.hexdigest(f)}")
            f.rewind
            IO.copy(f, zlib)
          end
        end
        data = io.to_s
        deflated = true
      end
      puts %(when #{path.lchop(ARGV[0]).inspect}\n  {Bytes.literal(#{data.bytes.join(", ")}), #{etag.inspect}, #{deflated}})
    end
  end
end

def already_compressed?(path)
  File.extname(path).in?(".webp", ".png")
end

recursive_bake(ARGV[0])
