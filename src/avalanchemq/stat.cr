module AvalancheMQ
  class Stat
    getter total, rate, frequency

    def initialize(@total = 0, @frequency = 1)
      @sample = @total
      @rate = 0
      spawn calc_rate
    end

    def increase
      @value += 1
      @sample += 1
    end

    def calc_rate
      loop do
        sleep @frequency
        @rate = @sample / @frequency
        @sample = 0
      end
    end
  end
end
