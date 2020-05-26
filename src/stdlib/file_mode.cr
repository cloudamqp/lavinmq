module Crystal::System::File
  # Helper method for calculating file open modes on systems with posix-y `open`
  # calls.
  private def self.open_flag(mode)
    if mode.size == 0
      raise "No file open mode specified"
    end

    m = 0
    o = 0
    case mode[0]
    when 'r'
      m = LibC::O_RDONLY
    when 'w'
      m = LibC::O_WRONLY
      o = LibC::O_CREAT | LibC::O_TRUNC
    when 'W'
      m = LibC::O_WRONLY
      o = LibC::O_CREAT
    when 'a'
      m = LibC::O_WRONLY
      o = LibC::O_CREAT | LibC::O_APPEND
    else
      raise "Invalid file open mode: '#{mode}'"
    end

    case mode.size
    when 1
      # Nothing
    when 2
      case mode[1]
      when '+'
        m = LibC::O_RDWR
      when 'b'
        # Nothing
      else
        raise "Invalid file open mode: '#{mode}'"
      end
    else
      raise "Invalid file open mode: '#{mode}'"
    end

    m | o
  end
end
