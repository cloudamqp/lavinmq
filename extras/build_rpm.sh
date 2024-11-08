#!/bin/bash

# Install rpmdevtools if not already installed
# so we can get rpmdev-setuptree to setup the rpmbuild tree, just in case
if which rpmdev-setuptree > /dev/null; then
    :
else
    sudo dnf install -y rpmdevtools
fi

rpmdev-setuptree

if [[ "$(basename $(pwd))" == "extras" ]]; then
    cd ..
fi

(
    make clean
)
tar czvf $HOME/rpmbuild/SOURCES/lavinmq.tar.gz -C .. $(basename $(pwd))
cp -f lavinmq.spec $HOME/rpmbuild/SPECS
rpmbuild -ba lavinmq.spec
