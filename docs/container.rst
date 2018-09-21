=================
Using a container
=================

Building the container
----------------------
You can create a Docker image suitable for running on OpenShift using the
`source-to-image`_ tool::

   sudo s2i build https:////github.com/fedora-infra/fedmsg-migration-tools registry.fedoraproject.org/f28/python3 fedmsg-migration-tools

.. _source-to-image: https://github.com/openshift/source-to-image

The produced image is capable of running the 3 included commands:

  - ``amqp-to-zmq``: the AMQP (fedora-messaging) to ZeroMQ (fedmsg) bridge
  - ``zmq-to-amqp``: the ZeroMQ (fedmsg) to AMQP (fedora-messaging) bridge
  - ``verify-missing``: the verification service that checks that all messages are
    sent to both networks.

Running the container
---------------------
You can choose which command the container will run by using the ``APP_SCRIPT``
environment variable. If you are running the container with Docker directly,
you can use a command similar to::

   sudo docker run -e APP_SCRIPT=./.s2i/amqp-to-zmq fedmsg-migration-tools

If you are running the container in OpenShift, set the environment variable in
the deployment options.

Configuration
-------------
If you're running the container in OpenShift, you can use the ``configmap``
feature to set the ``/etc/fedora-messaging/config.toml`` file to your liking.

If you are running the container in Docker, you can use the ``--volume`` option
of ``docker run``.
