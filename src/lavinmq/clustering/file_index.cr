module LavinMQ
  module Clustering
    module FileIndex
      abstract def files_with_hash(& : Tuple(String, Bytes) -> Nil)
      abstract def with_file(filename : String, & : MFile | File | Nil -> _)
    end
  end
end
