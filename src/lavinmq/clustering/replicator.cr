require "../mfile"

module LavinMQ
  module Clustering
    module Replicator
      abstract def register_file(file : File)
      abstract def register_file(mfile : MFile)
      abstract def replace_file(path : String) # only non mfiles are ever replaced
      abstract def append(path : String, file : MFile, position : Int32, length : Int32)
      abstract def append(path : String, obj)
      abstract def delete_file(path : String)
      abstract def followers : Array(Follower)
      abstract def close
      abstract def listen(server : TCPServer)
      abstract def clear
    end
  end
end
