Kafka
-----
>The purpose of this project is to test Kafka features.

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
5. brew install kafka

Service
-------
>Start:

1. brew services start zookeeper
2. brew services start kafka

>Stop:

1. brew services stop kafka
2. brew services stop zookeeper

Test
----
1. sbt clean test

Kafka
-----
* kafka-topics --zookeeper localhost:2181 --list
* kafka-topics --zookeeper localhost:2181 --delete --topic name