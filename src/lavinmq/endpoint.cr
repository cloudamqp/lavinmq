require "./endpoint/session"
require "./endpoint/local_session"
require "./endpoint/remote_session"

module LavinMQ
  module Endpoint
    # Opens nothing yet: returns the session for `uri`, in-process for a URI
    # without host (see `Endpoint.local?`), over AMQP otherwise. `origin` is
    # the vhost of the shovel or federation link; a local URI's vhost is
    # looked up from it.
    def self.session(uri : URI, origin : VHost, name : String) : Session
      if local?(uri)
        LocalSession.new(origin, vhost_name(uri), name)
      else
        RemoteSession.new(uri, name)
      end
    end
  end
end
