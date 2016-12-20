# Db2Kafka

[![Build Status](https://travis-ci.org/PagerDuty/db2kafka.svg?branch=master)](https://travis-ci.org/PagerDuty/db2kafka)

This service takes records from a database's `outbound_kafka_queue` table and
ships them to Kafka.

## Development


### Setup

#### Elixir

Short version: `iex -S mix` to run the service in development; `mix test --only unit --no-start` to run unit tests

#### MySQL

Setup the various database environment variables as per config/ before proceeding. You need a table called `outbound_kafka_queue` in your database:

```sql
CREATE TABLE `outbound_kafka_queue` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `topic` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `item_key` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `item_body` varbinary(60000) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

#### Kafka

Install [Kafka](http://kafka.apache.org/) if you don't already have it running!

Setup the various Kafka environment variables as per config/ before proceeding.

### Running Tests

#### Static Analysis

        mix dialyzer

Resolve all the warnings before submitting a PR!

To speed up `dialyzer`, build a Persistent Lookup Table (PLT):

        mix dialyzer.plt

#### Unit Tests

        mix test --only unit --no-start

#### Integration Tests

- Create MySQL database (see above for schema);

- Create Kafka topic:

        kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic foo --partitions 1 --replication-factor 1


- Run Integration Tests

        mix test --only integration --no-start


