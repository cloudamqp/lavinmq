module System
  def self.physical_memory
    {% if flag?(:linux) %}
      if max = cgroup_memory_max
        return max
      end
      LibC.get_phys_pages.to_u64 * LibC.getpagesize
    {% elsif flag?(:darwin) || flag?(:bsd) %}
      LibC.sysconf(LibC::SC_PHYS_PAGES) * LibC.sysconf(LibC::SC_PAGESIZE)
    {% else %}
      raise NotImplementedError.new("System.physical_memory")
    {% end %}
  end

  {% if flag?(:linux) %}
    @@cgroup_memory_max_path : String? = ""

    def self.cgroup_memory_max : UInt64?
      # Only look for the memory.max file once
      if @@cgroup_memory_max_path.try &.empty?
        @@cgroup_memory_max_path = nil
        if File.exists?("/proc/self/cgroup")
          if cgroup = File.read("/proc/self/cgroup").chomp.split("::", 2)[1]?
            path = "/sys/fs/cgroup#{cgroup}/memory.max"
            if File.exists?(path)
              @@cgroup_memory_max_path = path
            end
          end
        end
      end
      if path = @@cgroup_memory_max_path
        # memory.max can include the number of bytes or the word 'max'
        if max = File.read(path).to_u64?
          return max
        end
      end
    end
  {% end %}
end
