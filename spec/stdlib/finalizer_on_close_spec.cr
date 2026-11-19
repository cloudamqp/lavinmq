require "spec"
require "socket"
require "../../src/stdlib/*"

# Whether the GC still has a finalizer registered for the object
def finalizer_registered?(object : Reference) : Bool
  fn = Pointer(Void).null
  data = Pointer(Void).null
  none = LibGC::Finalizer.new(Pointer(Void).null, Pointer(Void).null)
  LibGC.register_finalizer_ignore_self(object.as(Void*), none, nil, pointerof(fn).as(LibGC::Finalizer*), pointerof(data))
  return false if fn.null?
  LibGC.register_finalizer_ignore_self(object.as(Void*), LibGC::Finalizer.new(fn, Pointer(Void).null), data, nil, nil)
  true
end

describe "finalizers on close" do
  it "keeps a file's finalizer while it's open and drops it on close" do
    file = File.tempfile("finalizer")
    begin
      finalizer_registered?(file).should be_true
      finalizer_registered?(file).should be_true # the probe put it back
      file.close
      finalizer_registered?(file).should be_false
      file.close # closing twice is fine
    ensure
      file.delete
    end
  end

  it "drops a socket's finalizer on close" do
    server = TCPServer.new("localhost", 0)
    client = TCPSocket.new("localhost", server.local_address.port)
    finalizer_registered?(client).should be_true
    client.close
    finalizer_registered?(client).should be_false
    server.close
    finalizer_registered?(server).should be_false
  end
end
