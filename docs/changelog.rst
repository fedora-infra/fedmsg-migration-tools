=============
Release Notes
=============

.. towncrier release notes start

0.1.2 (2018-09-13)
==================

Features
--------

* Add Systemd service files and an RPM spec file.
  (`PR#10 <https://github.com/fedora-infra/fedmsg-migration-tools/pull/10>`_)

* Add validation and signing for bridges.
  (`PR#2 <https://github.com/fedora-infra/fedmsg-migration-tools/pull/2>`_)


Bug Fixes
---------

* Fix up verify_missing config.
  (`PR#2 <https://github.com/fedora-infra/fedmsg-migration-tools/pull/2>`_)

* Wrap messages sent to ZMQ with fedmsg format.
  (`PR#9 <https://github.com/fedora-infra/fedmsg-migration-tools/pull/9>`_)


Development Changes
-------------------

* Use towncrier to generate the release notes, and check our
  dependencies' licenses.
  (`PR#10 <https://github.com/fedora-infra/fedmsg-migration-tools/pull/10>`_)

* Use Mergify.
  (`PR#7 <https://github.com/fedora-infra/fedmsg-migration-tools/pull/7>`_)

* Use `Black <https://github.com/ambv/black>`_.
  (`PR#8 <https://github.com/fedora-infra/fedmsg-migration-tools/pull/8>`_)


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Jeremy Cline


