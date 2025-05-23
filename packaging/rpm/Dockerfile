ARG build_image=84codes/crystal:latest-fedora-39

FROM $build_image AS builder
RUN dnf install -y --nodocs --setopt=install_weak_deps=False \
    rpmdevtools rpmlint systemd-rpm-macros make help2man lz4-devel
RUN rpmdev-setuptree
COPY packaging/rpm/lavinmq.spec /root/rpmbuild/SPECS/
ARG version
RUN rpmlint /root/rpmbuild/SPECS/lavinmq.spec
WORKDIR /usr/src/lavinmq
COPY Makefile README.md LICENSE NOTICE CHANGELOG.md shard.yml shard.lock ./
COPY extras/lavinmq.service extras/lavinmq.ini extras/
COPY static/ static/
COPY views/ views/
COPY src/ src/
RUN tar -czf /root/rpmbuild/SOURCES/lavinmq.tar.gz -C /usr/src lavinmq
ARG MAKEFLAGS=-j2
RUN rpmbuild -ba /root/rpmbuild/SPECS/lavinmq.spec
RUN rpmlint /root/rpmbuild/RPMS/* || true

FROM fedora:39 AS test
COPY --from=builder /root/rpmbuild/RPMS /tmp/RPMS
RUN find /tmp/RPMS -type f -exec dnf install -y {} \;
RUN lavinmq --version

# Copy the deb package to a scratch image, that then can be exported
FROM scratch
COPY --from=builder /root/rpmbuild/RPMS /root/rpmbuild/SRPMS .
