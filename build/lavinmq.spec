Name:    lavinmq
Version: 1.0.0
Release: 0.1beta7%{?dist}
Summary: Message queue server that implements the AMQP 0-9-1 protocol

License: ASL 2.0
BuildRequires: systemd-rpm-macros crystal help2man
Requires(pre): shadow-utils
URL: https://github.com/cloudamqp/lavinmq
Source: https://github.com/cloudamqp/lavinmq/archive/refs/tags/v1.0.0-beta.7.tar.gz

%description
A resource efficient message queue server implementing the AMQP protocol

%prep
%setup -n lavinmq-1.0.0-beta.7

%check
#crystal tool format --check
#crystal spec

%build
make -j2

%install
install -D -m 0755 bin/%{name} %{buildroot}/%{_bindir}/%{name}
install -D -m 0755 bin/%{name}-debug %{buildroot}/%{_bindir}/%{name}-debug
install -D -m 0755 bin/%{name}ctl %{buildroot}/%{_bindir}/%{name}ctl
install -D -m 0755 bin/%{name}perf %{buildroot}/%{_bindir}/%{name}perf
install -D -m 0644 extras/%{name}.service %{buildroot}/%{_unitdir}/%{name}.service
install -D -m 0644 extras/config.ini %{buildroot}/%{_sysconfdir}/%{name}/%{name}.ini
mkdir -p %{buildroot}/%{_mandir}/man1
help2man -Nn "fast and advanced message queue server" bin/lavinmq > %{buildroot}/%{_mandir}/man1/lavinmq.1
help2man -Nn "control utility for lavinmq server" bin/lavinmqctl > %{buildroot}/%{_mandir}/man1/lavinmqctl.1
help2man -Nn "performance testing tool for amqp servers" bin/lavinmqperf > %{buildroot}/%{_mandir}/man1/lavinmqperf.1
mkdir -p %{buildroot}/%{_sharedstatedir}/%{name}

%pre
getent group %{name} >/dev/null || groupadd -r %{name}
getent passwd %{name} >/dev/null || \
    useradd -r -g %{name} -d /nonexistent -s /sbin/nologin %{name}
exit 0

%post
%systemd_post %{name}.service

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun_with_restart %{name}.service

%files
%doc README.md NOTICE CHANGELOG.md
%license LICENSE
%{_bindir}/%{name}*
%{_unitdir}/%{name}.service
%{_mandir}/man1/*
%dir %attr(750, lavinmq, lavinmq) %{_sharedstatedir}/%{name}
%config(noreplace) %{_sysconfdir}/%{name}/%{name}.ini

%changelog
* Wed Jul 03 2019 CloudAMQP <contact@cloudamqp.com>
- Initial version of the package
