macro add_etcd_around_each
  around_each do |spec|
    p = Process.new("etcd", {
      "--data-dir=/tmp/clustering-spec.etcd",
      "--logger=zap",
      "--log-level=error",
      "--unsafe-no-fsync=true",
      "--force-new-cluster=true",
      "--listen-peer-urls=http://127.0.0.1:12380",
      "--listen-client-urls=http://127.0.0.1:12379",
      "--advertise-client-urls=http://127.0.0.1:12379",
    }, output: STDOUT, error: STDERR)

    client = HTTP::Client.new("127.0.0.1", 12379)
    i = 0
    loop do
      sleep 0.02.seconds
      response = client.get("/version")
      if response.status.ok?
        next if response.body.includes? "not_decided"
        break
      end
    rescue e : Socket::ConnectError
      i += 1
      raise "Cant connect to etcd on port 12379. Giving up after 100 tries. (#{e.message})" if i >= 100
      next
    end
    client.close
    begin
      spec.run
    ensure
      p.terminate(graceful: false) rescue nil
      FileUtils.rm_rf "/tmp/clustering-spec.etcd"
    end
  end
end
