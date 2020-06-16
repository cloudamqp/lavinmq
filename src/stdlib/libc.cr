# No PR yet
lib LibC
  {% if flag?(:linux) %}
    fun get_phys_pages : Int32
    fun getpagesize : Int32
  {% end %}

  {% if flag?(:darwin) %}
    SC_PAGESIZE   =  29
    SC_PHYS_PAGES = 200
  {% end %}
end

lib LibC
  {% if flag?(:linux) %}
    fun fallocate(fd : Int, mode : Int, offset : OffT, len : OffT) : Int
    FALLOC_FL_KEEP_SIZE      = 0x01
    FALLOC_FL_PUNCH_HOLE     = 0x02
    FALLOC_FL_NO_HIDE_STALE  = 0x04
    FALLOC_FL_COLLAPSE_RANGE = 0x08
    FALLOC_FL_ZERO_RANGE     = 0x10
    FALLOC_FL_INSERT_RANGE   = 0x20
    FALLOC_FL_UNSHARE_RANGE  = 0x40

    fun posix_fadvise(fd : Int, offset : OffT, len : OffT, advice : Int) : Int
    POSIX_FADV_NORMAL     = 0 # No further special treatment.
    POSIX_FADV_RANDOM     = 1 # Expect random page references.
    POSIX_FADV_SEQUENTIAL = 2 # Expect sequential page references.
    POSIX_FADV_WILLNEED   = 3 # Will need these pages.
    POSIX_FADV_DONTNEED   = 4 # Don't need these pages.
    POSIX_FADV_NOREUSE    = 5 # Data will be accessed once.
  {% end %}
end

lib LibC
  fun pwrite(fd : Int, buf : Void*, n : SizeT, offset : OffT) : SSizeT
  struct IoVec
    iov_base : Void*
    iov_len : SizeT
  end
  fun writev(fd : Int, iovec : IoVec*, n : SizeT) : SSizeT
  fun readv(fd : Int, iovec : IoVec*, n : SizeT) : SSizeT
end

lib LibC
  {% if flag?(:linux) %}
    fun sendfile(fd_out : Int, fd_in : Int, offset : OffT*, count : SizeT) : SSizeT
  {% else %}
    struct SendfileHeader
      headers : IoVec*
      hdr_cnt : Int
      trailers : IoVec*
      trl_cnt : Int
    end
    fun sendfile(fd : Int, s : Int, offset : OffT*, len : OffT*, hdtr : SendfileHeader*, flags : Int) : Int
  {% end %}
end

lib LibC
  {% if flag?(:linux) && compare_versions(`ldd --version | awk '/GLIBC/ { print $NF }'`.chomp + ".0", "2.27.0") >= 0 %}
    fun copy_file_range(fd_in : Int, offset_in : OffT*, fd_out : Int, offset_out : OffT*, len : SizeT, flags : UInt) : Int
  {% end %}
end
