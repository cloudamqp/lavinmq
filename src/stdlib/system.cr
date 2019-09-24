module System
  def self.physical_memory
    {% if flag?(:linux) %}
      LibC.get_phys_pages.to_u64 * LibC.getpagesize
    {% elsif flag?(:darwin) %}
      LibC.sysconf(LibC::SC_PHYS_PAGES) * LibC.sysconf(LibC::SC_PAGESIZE)
    {% else %}
      raise NotImplementedError.new("System.physical_memory")
    {% end %}
  end
end
