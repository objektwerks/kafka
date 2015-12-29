Kafka
-----
>The purpose of this project is to prototype and test Kafka features.

***

Homebrew
--------
>Install Homebrew on OSX. [How-To] (http://coolestguidesontheplanet.com/installing-homebrew-os-x-yosemite-10-10-package-manager-unix-apps/)

Installation
------------
>Install the following packages via Homebrew:

1. brew tap homebrew/services [Homebrew Services] (https://robots.thoughtbot.com/starting-and-stopping-background-services-with-homebrew)
2. brew install scala
3. brew install sbt
4. brew install zookeeper

Kafka
-----
>For Scala 2.10, **brew install kafka**. For Scala 2.11, download release ( http://kafka.apache.org/downloads.html )

1. edit $KAFKA_HOME/config/server.properties log.dirs=$KAFKA_HOME/logs
2. mkdir $KAFKA_HOME/logs

Environment
-----------
>The following environment variables should be in your .bash_profile

- export KAFKA_HOME="/Users/myhome/workspace/apache/kafka"
- export PATH=${JAVA_HOME}/bin:${KAFKA_HOME}/bin:/usr/local/bin:/usr/local/sbin:$PATH

Service
-------
>Start:

1. brew services start zookeeper
2. nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > $KAFKA_HOME/logs/kafka.nohup&

>Stop:

1. brew services stop zookeeper
2. $KAFKA_HOME/bin/kafka-server-stop.sh

Test
----
>See output at ./target/output/test.

1. sbt clean test
