# Linux page cache and mapping inspection, for specs of how segments are read

lib LibC
  fun mincore(addr : Void*, length : SizeT, vec : UInt8*) : Int
  fun posix_fadvise(fd : Int, offset : OffT, len : OffT, advice : Int) : Int
end

# Drops the pages of the file at `path` from the page cache, as on a cold start
def evict_from_page_cache(path : String) : Nil
  File.open(path) do |file|
    file.fsync # dirty pages aren't dropped
    advice = 4 # POSIX_FADV_DONTNEED
    raise "posix_fadvise failed" unless LibC.posix_fadvise(file.fd, 0, 0, advice).zero?
  end
end

# Pages of the file at `path` that are in the page cache
def cached_pages(path : String) : Int32
  page_size = LibC.sysconf(LibC::SC_PAGESIZE)
  MFile.open(path) do |mfile|
    slice = mfile.to_slice
    vec = Bytes.new((slice.size + page_size - 1) // page_size)
    raise "mincore failed" unless LibC.mincore(slice.to_unsafe, slice.size, vec).zero?
    vec.count { |b| b & 1 == 1 }
  end
end

# The flags (as in /proc/<pid>/smaps, e.g. "rr" for MADV_RANDOM) of the
# mapping of the file at `path`
def vm_flags(path : String) : Array(String)
  in_mapping = false
  File.each_line("/proc/self/smaps") do |line|
    if line.matches?(/^[0-9a-f]+-[0-9a-f]+ /)
      in_mapping = line.ends_with?(" #{path}")
    elsif in_mapping && line.starts_with?("VmFlags:")
      return line.split[1..]
    end
  end
  fail "#{path} isn't mapped"
end
