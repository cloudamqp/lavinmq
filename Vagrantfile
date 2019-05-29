# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "2048"
    vb.cpus = 2
  end

  config.vm.define "alpine" do |node|
    node.vm.box = "alpine/alpine64"
    node.vm.box_version = "3.6.0"
    node.vm.provision "shell", inline: <<~SHELL
      cat > /etc/apk/repositories << 'EOF'
      http://dl-cdn.alpinelinux.org/alpine/edge/main
      http://dl-cdn.alpinelinux.org/alpine/edge/community
      EOF
      apk upgrade --update-cache --available
      apk add --update crystal shards libc-dev libxml2-dev openssl-dev readline-dev gmp-dev yaml-dev
    SHELL
  end

  config.vm.define "ubuntu16" do |node|
    node.vm.box = "ubuntu/xenial64"
    node.vm.provision "shell", inline: <<~SHELL
      apt-key adv --keyserver keys.gnupg.net --recv-keys 09617FD37CC06B54
      echo "deb https://dist.crystal-lang.org/apt crystal main" > /etc/apt/sources.list.d/crystal.list
      apt-get update
      apt-get upgrade -y
      apt-get install -y crystal help2man lintian
    SHELL
  end

  config.vm.define "ubuntu18" do |node|
    node.vm.box = "ubuntu/bionic64"
    node.vm.provision "shell", inline: <<~SHELL
      apt-key adv --keyserver keys.gnupg.net --recv-keys 09617FD37CC06B54
      echo "deb https://dist.crystal-lang.org/apt crystal main" > /etc/apt/sources.list.d/crystal.list
      apt-get update
      apt-get upgrade -y
      apt-get install -y crystal help2man lintian
    SHELL
  end

  config.vm.define "debian9" do |node|
    node.vm.box = "debian/stretch64"
    node.vm.provision "shell", inline: <<~SHELL
      apt-get install dirmngr -y
      apt-key adv --keyserver keys.gnupg.net --recv-keys 09617FD37CC06B54
      echo "deb http://dist.crystal-lang.org/apt crystal main" > /etc/apt/sources.list.d/crystal.list
      apt-get update
      apt-get upgrade -y
      apt-get install -y crystal help2man lintian
    SHELL
  end
end
