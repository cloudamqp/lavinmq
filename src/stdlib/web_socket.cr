require "http/web_socket"

class HTTP::WebSocket
  def send(message : Bytes) : Nil
    check_open
    @ws.send(message)
  end
end
