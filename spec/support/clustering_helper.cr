class SpecClustering
  getter replicator, config, follower_config

  @follower_stopped = WaitGroup.new(1)
  @follower_config : LavinMQ::Config
  @stopped = false

  def initialize(@config : LavinMQ::Config, follower_data_dir : String)
    @replicator = LavinMQ::Clustering::Server.new(config, LavinMQ::Etcd.new("localhost:12379"), 0)
    tcp_server = TCPServer.new("localhost", 0)

    @follower_config = @config.dup.tap &.data_dir = follower_data_dir
    @repli = LavinMQ::Clustering::Client.new(@follower_config, 1, replicator.password, proxy: false)

    spawn(replicator.listen(tcp_server), name: "repli server spec")
    spawn(name: "follow spec") do
      @repli.follow("localhost", tcp_server.local_address.port)
      @follower_stopped.done
    end

    until replicator.followers.size == 1
      Fiber.yield
    end
  end

  def stop
    return if @stopped
    @stopped = true
    @replicator.close
    @repli.close
    @follower_stopped.wait
  end
end

def with_clustering(config = LavinMQ::Config.instance, &)
  follower_data_dir = File.tempname
  Dir.mkdir follower_data_dir
  yield clustering = SpecClustering.new(config, follower_data_dir)
ensure
  clustering.stop if clustering
  FileUtils.rm_rf follower_data_dir if follower_data_dir
end
