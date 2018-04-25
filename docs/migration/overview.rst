
==================
Migration Overview
==================

Fedora's infrastructure makes heavy use of the event-driven services. In this
pattern, one service emits a message when a particular event occurs and a
different service uses that event to trigger some work.

As a concrete example, release-monitoring.org polls upstream projects for new
versions. When it discovers a new version, it emits a message. When
the-new-hotness receives this message, it tries to automatically update the
project's spec file and build a new version for testing.


Migrate from what?
==================

Fedora currently sends messages using ZeroMQ. ZeroMQ provides a familiar socket
API and all the basic building blocks necessary for common (and not-so-common)
message passing patterns.


Why Migrate?
============

Our current usage of ZeroMQ is limited to its PUB/SUB sockets. There's no
broker. Although it's true that a broker introduces a point of failure, the
broker offers a number of useful features out of the box like guaranteed
delivery, durable message queues, authentication and authorization, monitoring,
etc.

It's true ZeroMQ can be used to achieve all the features a broker like RabbitMQ
provides. It's a great library to start with if if you're interested in building
a message broker. Fedora shouldn't try to build a broker, though.


The Plan
========

This plan assumes the following requirements:

* No flag day.

* Don't disrupt any services or applications.

* Don't break any services outside of Fedora's infrastructure relying on these
  messages.


Deploy a Broker
---------------

The first step is to deploy a broker in Fedora to use. The broker should support
(at a minimum) AMQP. RabbitMQ is probably a safe choice, but other brokers that
support AMQP (0.9.1) are fine.


Building Bridges
----------------

In order to avoid a flag day, bridges from AMQP to ZeroMQ and ZeroMQ to AMQP
need to be implemented and deployed. Some care needs to be taken to ensure
messages don't get looped endlessly between AMQP and ZeroMQ. In order to avoid
endless loops, two AMQP topic exchanges will be used. These will be used to
separate those messages originally published to ZeroMQ from those originally
published to AMQP. We'll call these exchanges "amq.topic" and "zmq.topic". The
setup is as follows:

1. The ZeroMQ to AMQP bridge publishes message to the "zmq.topic"
   exchange, using the ZeroMQ topic as the AMQP topic.

2. AMQP publishers publish to the "amq.topic" exchange.

3. The AMQP to ZeroMQ bridge binds a queue to the "amq.topic" exchange and
   publishes all messages to a ZeroMQ PUB socket. This socket is added to the
   list of sockets all fedmsg consumers connect to.

4. When a ZeroMQ consumer is migrated to AMQP, the queue it sets up is bound
   to both the "zmq.topic" and "amq.topic" with the topics it's interested in.

.. figure:: Fedora_AMQP_migration.svg
   :align: center
   :alt: Diagram of the AMQP and ZMQ bridges

   A diagram of how messages are routed using the AMQP <-> ZMQ bridging.

This allows both fedmsg publishers and subscribers to migrate at their leisure.
Once all Fedora services are migrated, the ZeroMQ to AMQP bridge can be turned
off. If Fedora wishes to continue supporting the external ZeroMQ interface, the
AMQP to ZeroMQ bridge should continue to be run.


Testing
-------

In order to validate that the bridges are functioning, a small service could be
run during the transition period that connects to fedmsg and to the AMQP queues
to compare messages. This will help catch format changes, configuration issues
that lead to message loss, etc. It will also likely have false positives since
ZeroMQ PUB/SUB sockets are designed to be unreliable and determining if a
message is lost or merely slow to be delivered is a difficult problem.


Converting Applications
-----------------------

After the bridges are running, applications are free to migrate. There are
several options when migrating and each has advantages and disadvantages.

Use fedmsg APIs
~~~~~~~~~~~~~~~

fedmsg already supports publishing and subscribing using STOMP. RabbitMQ
supports STOMP via a plugin. However, these APIs don't expose many features of
AMQP and are actually harder to use than the plain messaging APIs.
Additionally, the support for publishing and subscribing with STOMP is provided
by moksha, which is not actively maintained, largely undocumented, and offers a
set of unwieldy API. Moreover, it has a large chain of dependencies for
features we don't use.

The advantage to this approach is it requires very little additional work.
The downside is we have to continue to deal with APIs that hide functionality
without helping users, and we must be careful to not break the API for users of
ZeroMQ. Ultimately, we will need to migrate away from these APIs if we want to
take full advantage of the AMQP features.


Use AMQP Libraries Directly
~~~~~~~~~~~~~~~~~~~~~~~~~~~

One option is to have applications use Pika, Kombu, or some other AMQP library
directly to publish and subscribe. This provides a maximum amount of
flexibility to applications, but may result in a lot of helper patterns being
implemented across applications.


Create a New API
~~~~~~~~~~~~~~~~

Create a new API for users to migrate to rather than using fedmsg or migrating
directly to an AMQP library. This would ensure we don't break existing fedmsg
APIs, allow us to improve the interfaces, remove deprecated features, etc.
However, designing a good interface is not trivial and typically takes a few
tries to get right. It also requires careful testing, documentation, etc.

Features of this API could include:

* A method to define message schemas and offer automatic validation of messages
  using those schemas.

* A method to easily register SQLAlchemy database models that map to messages
  and automatically publish messages on database transaction commits.

* Boilerplate for typical publishers and consumers.


A Demo
======

It's all well and good to read about migrating to AMQP and bridges and queues,
but seeing it in action can be worth a thousand words. To demonstrate how easy
the bridging process is, I've developed some simple not-ready-for-production-yet
tools so you can set all this up at home.

1. Install RabbitMQ. On Fedora, ``sudo dnf install rabbitmq-server``

2. Start RabbitMQ: ``sudo systemctl start rabbitmq-server``

3. Enable the management plugin for a nice HTTP interface with ``sudo
   rabbitmq-plugins enable rabbitmq_management``

4. Navigate to http://localhost:15672/ and login. The default username is
   ``guest`` and the password is ``guest``.

5. Install the migration tools:

```
mkvirtualenv --python python3 fedmsg_migration_tools
pip install fedmsg-migration-tools
```

6. Start the ZeroMQ to AMQP bridge:

```
fedmsg-migration-tools zmq_to_amqp --zmq-endpoint "tcp://fedoraproject.org:9940" --zmq-endpoint "tcp://release-monitoring.org:9940"
```

7. In a second terminal, start the AMQP to ZeroMQ bridge:

```
workon fedmsg_migration_tools
fedmsg-migration-tools amqp_to_zmq
```

8. Congratulations, you now have a functional bridge to and from AMQP. ZeroMQ
   messages are being published to the "zmq.topic" exchange, and any messages
   published to the "amq.topic" are bridged to ZeroMQ. AMQP queues that bind to
   both the "amq.topic" and "zmq.topic" exchanges with their desired routing
   keys will get messages published from either AMQP or ZMQ
