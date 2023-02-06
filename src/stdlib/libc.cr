# No PR yet
lib LibC
  {% if flag?(:linux) %}
    fun get_phys_pages : Int32
  {% end %}

  fun getpagesize : Int32

  {% if flag?(:darwin) %}
    SC_PHYS_PAGES = 200
  {% elsif flag?(:freebsd) || flag?(:dragonfly) || flag?(:netbsd) %}
    SC_PHYS_PAGES = 121
  {% elsif flag?(:openbsd) %}
    SC_PHYS_PAGES = 500
  {% end %}
end

lib LibC
  fun posix_fadvise(fd : Int, offset : OffT, len : OffT, advice : Int) : Int
  POSIX_FADV_NORMAL     = 0 # No further special treatment.
  POSIX_FADV_RANDOM     = 1 # Expect random page references.
  POSIX_FADV_SEQUENTIAL = 2 # Expect sequential page references.
  POSIX_FADV_WILLNEED   = 3 # Will need these pages.
  POSIX_FADV_DONTNEED   = 4 # Don't need these pages.
  POSIX_FADV_NOREUSE    = 5 # Data will be accessed once.
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
