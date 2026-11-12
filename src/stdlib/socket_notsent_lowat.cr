require "socket"

class Socket
  # Sets `TCP_NOTSENT_LOWAT`: the socket reports writable only once the data
  # it hasn't sent yet drops below this, instead of once about a third of the
  # (autotuned, up to `tcp_wmem` max) send buffer is free. A blocked write then
  # resumes after the peer has read a little, so a write timeout measures
  # whether the peer reads at all, not whether it reads fast. Ignored where
  # the option isn't supported.
  def tcp_notsent_lowat=(bytes : Int32) : Int32
    {% if flag?(:linux) %}
      setsockopt 25, bytes, level: LibC::IPPROTO_TCP # TCP_NOTSENT_LOWAT
    {% elsif flag?(:darwin) %}
      setsockopt 0x201, bytes, level: LibC::IPPROTO_TCP # TCP_NOTSENT_LOWAT
    {% end %}
    bytes
  rescue Socket::Error
    bytes
  end
end
