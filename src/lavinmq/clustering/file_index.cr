module LavinMQ
  module Clustering
    module FileIndex
      abstract def files_with_hash(algo : Digest, & : Tuple(String, Bytes) -> Nil)
      abstract def with_file(filename : String, & : MFile | File | Nil -> Nil) : Nil
    end
  end
end
