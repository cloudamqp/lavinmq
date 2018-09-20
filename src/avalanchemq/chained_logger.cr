require "logger"

module AvalancheMQ
  class ChainedLogger < ::Logger
    property parent
    @parent : Logger?

    def level
      @parent ? @parent.not_nil!.level : super
    end

    def dup
      l = ChainedLogger.new(@io, formatter: @formatter, progname: @progname)
      l.parent = self
      l
    end
  end
end
