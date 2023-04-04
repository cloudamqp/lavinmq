require "digest/md5"

def recursive_bake(dir)
  Dir.each_child(dir) do |child|
    path = File.join(dir, child)
    if File.directory? path
      recursive_bake path
    else
      bytes = File.read(path)
      etag = Digest::MD5.hexdigest(bytes)
      puts %("#{path.lchop(ARGV[0])}": {#{bytes.inspect}.to_slice, #{etag.inspect}},)
    end
  end
end

recursive_bake(ARGV[0])
