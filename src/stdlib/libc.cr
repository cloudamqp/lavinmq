# No PR yet
lib LibC
  {% if flag?(:linux) %}
    fun get_phys_pages : Int32
    fun syncfs(fd : Int) : Int
  {% end %}

  fun sync : Void
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
  fun pwrite(fd : Int, buf : Void*, n : SizeT, offset : OffT) : SSizeT

  struct IoVec
    iov_base : Void*
    iov_len : SizeT
  end

  fun writev(fd : Int, iovec : IoVec*, n : SizeT) : SSizeT
  fun readv(fd : Int, iovec : IoVec*, n : SizeT) : SSizeT
end
