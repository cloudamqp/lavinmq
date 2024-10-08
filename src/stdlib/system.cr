module System
  def self.physical_memory
    {% if flag?(:linux) %}
      LibC.get_phys_pages.to_u64 * LibC.getpagesize
    {% elsif flag?(:darwin) || flag?(:bsd) %}
      LibC.sysconf(LibC::SC_PHYS_PAGES) * LibC.sysconf(LibC::SC_PAGESIZE)
    {% elsif flag?(:windows) %}
      memory = LibC::MEMORYSTATUSEX.new
      memory.dwLength = sizeof(LibC::MEMORYSTATUSEX)
      ptr = pointerof(memory)
      mem = LibC.GlobalMemoryStatusEx(ptr)
      memory.ullTotalPhys
    {% else %}
      raise NotImplementedError.new("System.physical_memory")
    {% end %}
  end
end
