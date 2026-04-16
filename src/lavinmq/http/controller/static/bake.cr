require "digest/md5"
require "compress/zlib"

def recursive_bake(dir)
  Dir.each_child(dir) do |child|
    path = File.join(dir, child)
    if File.directory? path
      recursive_bake path
    else
      File.open(path) do |f|
        etag = %(W/"#{Digest::MD5.hexdigest(f)}")
        f.rewind
        if already_compressed?(path)
          data = String.build(f.size) { |s| IO.copy(f, s) }
          deflated = false
        else
          data = String.build(f.size) do |io|
            Compress::Zlib::Writer.open(io, Compress::Zlib::BEST_COMPRESSION) do |zlib|
              IO.copy(f, zlib)
            end
          end
          deflated = true
          # Only use the deflated version if it's actually smaller than the original
          if data.bytesize >= File.size(path)
            f.rewind
            data = String.build(f.size) { |s| IO.copy(f, s) }
            deflated = false
          end
        end
        puts %(when #{path.lchop(ARGV[0]).inspect}\n  {Bytes.literal(#{data.bytes.join(", ")}), #{etag.inspect}, #{deflated}})
      end
    end
  end
end

def already_compressed?(path)
  File.extname(path).in?(".webp", ".png", ".woff2")
end

recursive_bake(ARGV[0])
