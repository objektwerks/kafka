Kafka
-----
>Kafka feature tests.

Installation
------------
1. brew tap homebrew/services
2. brew install scala
3. brew install sbt
4. brew install kafka ( which installs zookeeper )

Service
-------
1. brew services start zookeeper & kafka
2. brew services stop kafka & zookeeper

Warning
-------
>Starting zookeeper via homebrew does not always work as expected.
>So alternatively start both services as follows:
1. zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties &
2. kafka-server-start /usr/local/etc/kafka/server.properties &

Test
----
1. sbt clean test

Kafka
-----
>topics: keyvalue, keyvalue-tx

* kafka-topics --zookeeper localhost:2181 --list
* kafka-topics --zookeeper localhost:2181 --describe --topic keyvalue
* kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic keyvalue --time -1
* kafka-consumer-groups --bootstrap-server localhost:9092 --group objektwerks-group --describe
* kafka-topics --zookeeper localhost:2181 --delete --topic keyvalue
* kafka-consumer-groups --bootstrap-server localhost:9092 --list
* kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group objektwerks-group