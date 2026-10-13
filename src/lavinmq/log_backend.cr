require "log"

module LavinMQ
  # A Log::IOBackend that a config reload can close while fibers still log to it:
  # their entries are dropped instead of raising in the logging fiber
  class LogBackend < ::Log::IOBackend
    def dispatch(entry : ::Log::Entry) : Nil
      super
    rescue Channel::ClosedError
    end
  end
end
