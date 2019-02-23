lib LibC
  alias FsidT = StaticArray(Int32, 2)

  {% if flag?(:linux) %}
    struct Statfs
      type : Int64
      bsize : Int64
      blocks : UInt64
      bfree : UInt64
      bavail : UInt64
      files : UInt64
      ffree : UInt64
      fsid : FsidT
      namelen : Int64
      frsize : Int64
      flags : Int64
      spare : StaticArray(Long, 4)
    end
  {% elsif flag?(:darwin) %}
    struct Statfs
      bsize : UInt32
      iosize : Int32
      blocks : UInt64
      bfree : UInt64
      bavail : UInt64
      files : UInt64
      ffree : UInt64
      fsid : FsidT
      owner : UInt32
      type : UInt32
      flags : UInt32
      fssubtype : UInt32
      fstypename : StaticArray(Int16, 16)
      mntonname : StaticArray(Int16, 1024)
      mntfromname : StaticArray(Int16, 1024)
      reserved : StaticArray(Long, 8)
    end
  {% else %}
    {% raise "No statfs implementation" %}
  {% end %}

  fun statfs64(file : Char*, buf : Statfs*) : Int
end

struct FilesystemInfo
  def initialize(statfs : LibC::Statfs)
    @available = statfs.bavail * statfs.bsize
    @total = statfs.blocks * statfs.bsize
  end

  # Bytes available to non-privileged on disk
  getter available : UInt64

  # Disk size in bytes
  getter total : UInt64
end

module Filesystem
  def self.info(path)
    statfs = uninitialized LibC::Statfs
    unless LibC.statfs64(path.check_no_null_byte, pointerof(statfs)).zero?
      raise Errno.new("Error getting statfs")
    end
    FilesystemInfo.new(statfs)
  end
end
