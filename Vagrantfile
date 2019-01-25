# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "1024"
  end

  config.vm.define "alpine" do |node|
    node.vm.box = "alpine/alpine64"
    node.vm.box_version = "3.6.0"
    node.vm.provision "shell", inline: <<~SHELL
      apk update
      apk upgrade
      apk add crystal shards libc-dev libxml2-dev openssl-dev readline-dev gmp-dev yaml-dev
    SHELL
  end

  config.vm.define "ubuntu" do |node|
    node.vm.box = "ubuntu/bionic64"
    node.vm.provision "shell", inline: <<~SHELL
      apt-key adv --keyserver keys.gnupg.net --recv-keys 09617FD37CC06B54
      echo "deb https://dist.crystal-lang.org/apt crystal main" > /etc/apt/sources.list.d/crystal.list
      apt-get update
      apt-get install -y crystal help2man lintian
    SHELL
  end
end
