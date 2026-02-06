FROM archlinux:latest AS builder
RUN pacman -Syu --noconfirm base-devel crystal shards help2man curl lz4 namcap
WORKDIR /usr/src/lavinmq/src
COPY Makefile README.md LICENSE NOTICE CHANGELOG.md shard.yml shard.lock ./
COPY extras/lavinmq.service extras/lavinmq.ini extras/lavinmq.sysusers extras/
COPY static/ static/
COPY views/ views/
COPY src/ src/
WORKDIR /usr/src/lavinmq
COPY packaging/arch/PKGBUILD .
ARG version
RUN sed -i "s/pkgver=.*/pkgver=${version}/" PKGBUILD
RUN namcap PKGBUILD
RUN chown -R alpm:alpm /usr/src/lavinmq
USER alpm
ARG MAKEFLAGS=-j2
RUN makepkg -sf --noconfirm
RUN namcap *.pkg.tar.zst || true

FROM archlinux:latest AS test
COPY --from=builder /usr/src/lavinmq/*.pkg.tar.zst .
RUN pacman -Syu --noconfirm && pacman -U --noconfirm ./*.pkg.tar.zst
RUN lavinmq --version

# Copy the package to a scratch image, that then can be exported
FROM scratch
COPY --from=builder /usr/src/lavinmq/*.pkg.tar.zst .
