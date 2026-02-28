ARG build_image=84codes/crystal:latest-fedora-42
ARG test_image=fedora:42

FROM $build_image AS builder
# Enable CRB repository for el-9 to access help2man
RUN if grep -q 'PLATFORM_ID="platform:el9"' /etc/os-release; then \
      dnf install -y dnf-plugins-core && \
      dnf config-manager --enable crb; \
    fi
RUN dnf install -y --nodocs --setopt=install_weak_deps=False \
    rpmdevtools rpmlint systemd-rpm-macros make help2man lz4-devel
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
ARG MAKEFLAGS=-j2
RUN rpmbuild -ba /root/rpmbuild/SPECS/lavinmq.spec
RUN rpmlint /root/rpmbuild/RPMS/* || true

FROM $test_image AS test
COPY --from=builder /root/rpmbuild/RPMS /tmp/RPMS
RUN find /tmp/RPMS -type f -exec dnf install -y {} \;
RUN lavinmq --version
RUN getent passwd lavinmq

# Copy the RPM packages to a scratch image, that then can be exported.
# Copying from 'test' (not 'builder') ensures the test stage is executed.
FROM scratch
COPY --from=test /tmp/RPMS .
COPY --from=builder /root/rpmbuild/SRPMS .
