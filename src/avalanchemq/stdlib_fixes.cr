class Fiber
  def self.list(&blk : Fiber -> Nil)
    @@fibers.unsafe_each(&blk)
  end
end

require "openssl"
require "io"

abstract class OpenSSL::SSL::Socket
  def read_timeout=(read_timeout)
    io = @bio.io
    if io.responds_to? :read_timeout
      io.read_timeout = read_timeout
    else
      raise NotImplementedError.new("#{io.class}#read_timeout")
    end
  end

  def write_timeout=(write_timeout)
    io = @bio.io
    if io.responds_to? :write_timeout
      io.write_timeout = write_timeout
    else
      raise NotImplementedError.new("#{io.class}#write_timeout")
    end
  end
end

lib LibC
  {% if flag?(:linux) %}
    fun get_phys_pages : Int32
    fun getpagesize : Int32
  {% end %}

  {% if flag?(:darwin) %}
    SC_PAGESIZE = 29
    SC_PHYS_PAGES = 200
  {% end %}
end

module System
  def self.physical_memory
    {% if flag?(:linux) %}
      LibC.get_phys_pages * LibC.getpagesize
    {% elsif flag?(:darwin) %}
      LibC.sysconf(LibC::SC_PHYS_PAGES) * LibC.sysconf(LibC::SC_PAGESIZE)
    {% else %}
      raise NotImplementedError.new("System.physical_memory")
    {% end %}
  end
end
