
<!-- prettier-ignore-start -->

This guide will help you set up a 3-node LavinMQ cluster with [Docker Compose](https://docs.docker.com/compose/). 

This will set up a development cluster with 3 LavinMQ nodes and 1 etcd node. LavinMQ uses etcd to track which nodes are in-sync. Note that we suggest running multiple etcd nodes in a production environment. For more information on clustering, see the [clustering docs](clustering.md).

## Configuration

docker-compose.yml
{% highlight yaml %}
{% raw %}
name: lavinmq-cluster
services:
  lavinmq1:
    image: cloudamqp/lavinmq:latest
    hostname: lavinmq1
    container_name: lavinmq1
    ports:
      - 5672:5672
      - 5679:5679
      - 15672:15672
    volumes:
      - lavinmq-data:/var/lib/lavinmq/data
    configs:
      - source: lavinmq_config
        target: /etc/lavinmq/lavinmq.ini
    depends_on:
      etcd:
        condition: service_healthy
    networks:
      - network

  lavinmq2:
    image: cloudamqp/lavinmq:latest
    hostname: lavinmq2
    container_name: lavinmq2
    ports:
      - 5673:5672
      - 15679:5679
      - 25672:15672
    volumes:
      - lavinmq-data:/var/lib/lavinmq/data
    configs:
      - source: lavinmq_config
        target: /etc/lavinmq/lavinmq.ini
    depends_on:
      etcd:
        condition: service_healthy
    networks:
      - network

  lavinmq3:
    image: cloudamqp/lavinmq:latest
    hostname: lavinmq3
    container_name: lavinmq3
    ports:
      - 5674:5672
      - 25679:5679
      - 35672:15672
    volumes:
      - lavinmq-data:/var/lib/lavinmq/data
    configs:
      - source: lavinmq_config
        target: /etc/lavinmq/lavinmq.ini
    depends_on:
      etcd:
        condition: service_healthy
    networks:
      - network

  etcd:
    image: gcr.io/etcd-development/etcd:v3.6.5
    container_name: etcd
    ports:
      - 2379:2379
      - 2380:2380
    command:
      - /usr/local/bin/etcd
      - --name=etcd
      - --data-dir=/var/lib/etcd/data
      - --listen-client-urls=http://0.0.0.0:2379
      - --listen-peer-urls=http://0.0.0.0:2380
      - --initial-advertise-peer-urls=http://etcd:2380
      - --advertise-client-urls=http://etcd:2379
      - --initial-cluster=etcd=http://etcd:2380
      - --initial-cluster-state=new
      - --initial-cluster-token=etcd-cluster-0
    volumes:
      - etcd-data:/var/lib/etcd/data
    networks:
      - network
    healthcheck:
      test: [ "CMD", "etcdctl", "--endpoints=http://localhost:2379", "endpoint", "health" ]
      interval: 5s
      timeout: 5s
      retries: 3
    environment:
      - ETCDCTL_API=3

configs:
  lavinmq_config:
    content: |
        [clustering]
        enabled = true
        bind = 0.0.0.0
        etcd_endpoints = etcd:2379

networks:
  network:
    driver: bridge

volumes:
  lavinmq-data:
  etcd-data:

{% endraw %}
{% endhighlight %}


## Running the cluster

Start the containers by running:
{% highlight shell %}
{% raw %}
docker compose up
{% endraw %}
{% endhighlight %}

You can now start publishing and consuming messages at `amqp://guest:guest@localhost`, and access the management UI by visiting `http://localhost:15672`

Visit [cloudamqp/lavinmq](https://hub.docker.com/r/cloudamqp/lavinmq) on Docker Hub to learn more.

<!-- prettier-ignore-end -->
