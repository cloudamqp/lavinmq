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
