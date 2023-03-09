def recursive_bake(dir)
  Dir.each_child(dir) do |child|
    path = File.join(dir, child)
    if File.directory? path
      recursive_bake path
    else
      puts %("#{path.lchop(ARGV[0])}": #{File.read(path).inspect}.to_slice,)
    end
  end
end

recursive_bake(ARGV[0])
