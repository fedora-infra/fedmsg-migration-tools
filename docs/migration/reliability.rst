
===========
Reliability
===========

RabbitMQ deployments support `clustering`_, `high availability queues`_, and
`federation`_. Since Fedora does not want a single point of failure or regular
downtime for updates, it would be best to use clustering with high availability
queues.

Clustering
==========

For complete details, consult the `clustering`_ documentation. A few things to
note:

* All nodes in the cluster need to be running the same minor (same y version in
  a x.y.z release) so a major upgrade to Erlang requires downtime.

* Clustering should only be used on a LAN. See the documentation on `network
  partitions`_ for details. `Federation`_ is designed for sharing across WANs.


High Availability Queues
========================

RabbitMQ supports mirroring queues across nodes in a cluster in order to provide `high availability queues`_.

Again, consult RabbitMQ documentation for the details, but a few highlights are:

* Mirroring can be applied to a subset or all queues

* Queues can be mirrored to all nodes in a cluster or just a few.


.. _clustering: https://www.rabbitmq.com/clustering.html
.. _high availability queues: https://www.rabbitmq.com/ha.html
.. _federation: https://www.rabbitmq.com/federation.html
.. _network partitions: https://www.rabbitmq.com/partitions.html
