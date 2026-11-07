# Fail fast with a clear message when the etcd binary isn't installed,
# instead of each etcd spec burning its connect-retry loop before failing.
def ensure_etcd_in_path!
  return if Process.find_executable("etcd")
  raise "etcd binary not found in PATH, required by etcd-tagged specs. " \
        "Install etcd or exclude these specs with --tag=~etcd"
end

# Raft timing (ms) for spec etcd members. etcd's defaults (100/1000) are tuned
# for real networks; on loopback a shorter election timeout makes a new cluster
# elect its leader sooner and its members notice a lost quorum sooner.
ETCD_SPEC_TIMING_FLAGS = {"--heartbeat-interval=25", "--election-timeout=250"}

def with_etcd(&)
  ensure_etcd_in_path!
  p = Process.new("etcd", {
    "--data-dir=/tmp/clustering-spec.etcd",
    "--logger=zap",
    "--log-level=error",
    "--unsafe-no-fsync=true",
    "--force-new-cluster=true",
    *ETCD_SPEC_TIMING_FLAGS,
    "--listen-peer-urls=http://127.0.0.1:12380",
    "--listen-client-urls=http://127.0.0.1:12379",
    "--advertise-client-urls=http://127.0.0.1:12379",
  }, output: STDOUT, error: STDERR)

  begin
    client = HTTP::Client.new("127.0.0.1", 12379)
    i = 0
    loop do
      sleep 0.02.seconds
      response = client.get("/version")
      if response.status.ok?
        next if response.body.includes? "not_decided"
        break
      end
    rescue e : IO::Error
      client.close
      i += 1
      raise "Can't connect to etcd on port 12379. Giving up after 100 tries. (#{e.message})" if i >= 100
      next
    end
    client.close
    yield
  ensure
    p.terminate(graceful: false) rescue nil
    FileUtils.rm_rf "/tmp/clustering-spec.etcd"
    i = 0
    until p.terminated?
      break if (i &+= 1) > 200
      sleep 10.milliseconds
    end
  end
end

macro add_etcd_around_each
  around_each do |spec|
    with_etcd { spec.run }
  end
end
