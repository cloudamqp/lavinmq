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

    fun statfs64(file : Char*, buf : Statfs*) : Int
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

    fun statfs64(file : Char*, buf : Statfs*) : Int
  {% elsif flag?(:freebsd) %}
    # https://www.freebsd.org/cgi/man.cgi?query=statfs&manpath=FreeBSD+12.2-RELEASE
    struct Statfs
      version : UInt32                       # structure version number
      type : UInt32                          #	type of	filesystem
      flags : UInt64                         # copy of	mount exported flags
      bsize : UInt64                         # 	filesystem fragment size
      iosize : UInt64                        # 	optimal	transfer block size
      blocks : UInt64                        # 	total data blocks in filesystem
      bfree : UInt64                         # 	free blocks in filesystem
      bavail : Int64                         # 	free blocks avail to non-superuser
      files : UInt64                         # 	total file nodes in filesystem
      ffree : Int64                          # 	free nodes avail to non-superuser
      syncwrites : UInt64                    # 	count of sync writes since mount
      asyncwrites : UInt64                   # 	count of async writes since mount
      syncreads : UInt64                     # 	count of sync reads since mount
      asyncreads : UInt64                    # 	count of async reads since mount
      spare : StaticArray(UInt64, 10)        # [10]	unused spare
      namemax : UInt32                       # 	maximum	filename length
      owner : UidT                           # user that mounted the filesystem
      fsid : StaticArray(Int32, 2)           # filesystem id
      charspare : StaticArray(UInt8, 80)     # /* spare string space
      fstypename : StaticArray(UInt8, 16)    # [MFSNAMELEN] /* filesystem type name
      mntfromname : StaticArray(UInt8, 1024) # [MNAMELEN]	 /* mounted filesystem
      mntonname : StaticArray(UInt8, 1024)   # [MNAMELEN]	 /* directory on which mounted
    end

    fun statfs(file : Char*, buf : Statfs*) : Int
  {% else %}
    {% raise "No statfs implementation" %}
  {% end %}
end

struct FilesystemInfo
  def initialize(statfs : LibC::Statfs)
    @available = statfs.bavail.to_u64 * statfs.bsize
    @total = statfs.blocks.to_u64 * statfs.bsize
  end

  # Bytes available to non-privileged on disk
  getter available : UInt64

  # Disk size in bytes
  getter total : UInt64
end

{% if flag?(:linux) || flag?(:darwin) %}
  module Filesystem
    def self.info(path)
      statfs = uninitialized LibC::Statfs
      unless LibC.statfs64(path.check_no_null_byte, pointerof(statfs)).zero?
        raise File::Error.from_errno("Error getting statfs", file: path)
      end
      FilesystemInfo.new(statfs)
    end
  end
{% elsif flag?(:freebsd) %}
  module Filesystem
    def self.info(path)
      statfs = uninitialized LibC::Statfs
      unless LibC.statfs(path.check_no_null_byte, pointerof(statfs)).zero?
        raise File::Error.from_errno("Error getting statfs", file: path)
      end
      FilesystemInfo.new(statfs)
    end
  end
{% end %}
