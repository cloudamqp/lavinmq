ARG build_image=84codes/crystal:latest-fedora-43
ARG test_image=fedora:43

FROM $build_image AS builder
# Enable CRB repository for el-9 to access help2man
RUN if grep -q 'PLATFORM_ID="platform:el9"' /etc/os-release; then \
      dnf install -y dnf-plugins-core && \
      dnf config-manager --enable crb; \
    fi
RUN dnf install -y --nodocs --setopt=install_weak_deps=False \
    rpmdevtools rpmlint systemd-rpm-macros make help2man git lz4-devel openssl openssl-devel zlib-devel
RUN rpmdev-setuptree
COPY packaging/rpm/lavinmq.spec /root/rpmbuild/SPECS/
ARG version
RUN rpmlint /root/rpmbuild/SPECS/lavinmq.spec
WORKDIR /usr/src/lavinmq
COPY Makefile README.md LICENSE NOTICE CHANGELOG.md shard.yml shard.lock ./
COPY extras/lavinmq.service extras/lavinmq.ini extras/lavinmq.sysusers extras/
COPY static/ static/
COPY views/ views/
COPY src/ src/
RUN tar -czf /root/rpmbuild/SOURCES/lavinmq.tar.gz -C /usr/src lavinmq
COPY extras/lavinmq.sysusers /root/rpmbuild/SOURCES/
ARG MAKEFLAGS=-j2
RUN rpmbuild -ba /root/rpmbuild/SPECS/lavinmq.spec
RUN rpmlint /root/rpmbuild/RPMS/* || true

FROM $test_image AS test
COPY --from=builder /root/rpmbuild/RPMS /tmp/RPMS
RUN find /tmp/RPMS -type f -exec dnf install -y {} \;
RUN lavinmq --version
RUN getent passwd lavinmq

# Export from the test stage, otherwise BuildKit skips the install test
FROM scratch
COPY --from=test /tmp/RPMS .
COPY --from=builder /root/rpmbuild/SRPMS .
