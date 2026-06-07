module LavinMQ
  module Clustering
    module FileIndex
      abstract def files_with_hash(caps : Hash(String, Int64)?, & : Tuple(String, Bytes) -> Nil)
      abstract def with_file(filename : String, cap : Int64?, & : File?, Int64 -> _)
    end
  end
end
