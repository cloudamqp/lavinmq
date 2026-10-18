# No PR yet
lib LibC
  {% if flag?(:linux) %}
    SCHED_BATCH = 3

    struct SchedParam
      sched_priority : Int
    end

    # pid 0 is the calling thread
    fun sched_setscheduler(pid : PidT, policy : Int, param : SchedParam*) : Int
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
