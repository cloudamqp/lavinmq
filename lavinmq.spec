Name:    lavinmq
Summary: Message queue server that implements the AMQP 0-9-1 protocol
Version: 2.0.0
Release: 1%{?dist}

License: Apache 2.0
BuildRequires: systemd-rpm-macros crystal curl help2man lz4-devel openssl-devel
Requires(pre): shadow-utils
Suggests: etcd
URL: https://github.com/cloudamqp/lavinmq
Source: lavinmq.tar.gz

%description
A resource efficient message queue server implementing the AMQP protocol

%prep
%setup -qn lavinmq

%check

%build
make

%install
make install DESTDIR=%{buildroot} UNITDIR=%{_unitdir}

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
%doc README.md NOTICE
%doc %{_docdir}/%{name}/changelog
%license LICENSE
%{_bindir}/%{name}*
%{_unitdir}/%{name}.service
%{_mandir}/man1/*
%dir %attr(750, lavinmq, lavinmq) %{_sharedstatedir}/%{name}
%config(noreplace) %{_sysconfdir}/%{name}/%{name}.ini

%changelog
* Wed Jul 03 2019 CloudAMQP <contact@cloudamqp.com>
- Initial version of the package
