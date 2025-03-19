# To prevent dangling child processes when the main process crashes
# prctl has to be called in between fork and exec
{% if flag?(:linux) %}
  lib LibC
    fun prctl(op : Int, ...) : Int
    PR_SET_PDEATHSIG = 1
  end

  module Crystal::System::Signal
    def self.after_fork_before_exec
      previous_def

      LibC.prctl(LibC::PR_SET_PDEATHSIG, LibC::SIGTERM)
    end
  end
{% end %}
