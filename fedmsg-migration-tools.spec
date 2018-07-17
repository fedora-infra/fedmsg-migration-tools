%global pkgname fedmsg-migration-tools
%global srcname fedmsg_migration_tools

Name:           %{pkgname}
Version:        0.1.1
Release:        1%{?dist}
Summary:        Set of tools to aid in migrating from fedmsg to AMQP

License:        GPLv2+
URL:            https://github.com/fedora-infra/fedmsg-migration-tools
Source0:        %{url}/archive/v%{version}/%{srcname}-%{version}.tar.gz

BuildArch:      noarch
BuildRequires:  python%{python3_pkgversion}-devel
BuildRequires:  python%{python3_pkgversion}-click
BuildRequires:  python%{python3_pkgversion}-fedmsg
BuildRequires:  python%{python3_pkgversion}-twisted
BuildRequires:  python%{python3_pkgversion}-txzmq
BuildRequires:  python%{python3_pkgversion}-fedora-messaging
BuildRequires:  python%{python3_pkgversion}-pytoml

BuildRequires:  python-sphinx

Requires(pre): shadow-utils
%{?systemd_requires}
BuildRequires:  systemd

%{?python_enable_dependency_generator}

%description
Tools for migrating away from fedmsg.


%package doc
Summary:        Documentation for %{pkgname}
%description doc
Documentation for %{pkgname}.


%prep
%autosetup -n %{srcname}-%{version}


%build
%py3_build
# generate docs
PYTHONPATH=${PWD} sphinx-build -M html -d docs/_build/doctrees docs docs/_build/html
# remove the sphinx-build leftovers
rm -rf docs/_build/*/.buildinfo


%install
%py3_install
install -D -m 644 config.toml.example $RPM_BUILD_ROOT%{_sysconfdir}/%{name}/config.toml

# Systemd
install -D -m 644 systemd/%{name}.sysconfig $RPM_BUILD_ROOT%{_sysconfdir}/sysconfig/%{name}
mkdir -p $RPM_BUILD_ROOT%{_unitdir}/
for service in amqp-to-zmq zmq-to-amqp verify-missing; do
    install -m 644 systemd/fedmsg-${service}.service $RPM_BUILD_ROOT%{_unitdir}/
done


%pre
getent group fedmsg >/dev/null || groupadd -r fedmsg
getent passwd fedmsg >/dev/null || \
    useradd -r -g fedmsg -d /usr/share/doc/fedora-messaging -s /sbin/nologin \
    -c "Fedora Messaging" fedmsg

%post
for service in amqp-to-zmq zmq-to-amqp verify-missing; do
    %systemd_post fedmsg-${service}.service
done

%preun
for service in amqp-to-zmq zmq-to-amqp verify-missing; do
    %systemd_preun fedmsg-${service}.service
done

%postun
for service in amqp-to-zmq zmq-to-amqp verify-missing; do
    %systemd_postun_with_restart fedmsg-${service}.service
done


%files
%license LICENSE
%doc README.rst docs/migration/*
%{python3_sitelib}/*
%config(noreplace) %{_sysconfdir}/%{name}/config.toml
%config(noreplace) %{_sysconfdir}/sysconfig/%{name}
%{_bindir}/%{name}
%{_unitdir}/*.service

%files doc
%license LICENSE
%doc README.rst docs/*.rst docs/_build/html


%changelog
* Mon Jul 16 2018 Aurelien Bompard <abompard@fedoraproject.org>
- Initial package
