# Db2Kafka

[![Build Status](https://travis-ci.com/PagerDuty/web2kafka.svg?token=7Mi8LhmhpJYhzs4euq1w&branch=master)](https://travis-ci.com/PagerDuty/web2kafka)

This service takes records from a database's `outbound_kafka_queue` table and
ships them to Kafka.

## Development


### Setup

#### Elixir

For Elixir development setup and standard project commands, see [Elixir Development Notes](https://pagerduty.atlassian.net/wiki/display/~div/Elixir+Development+Notes)

Short version: `iex -S mix` to run the service in development; `mix test --only unit --no-start` to run unit tests

#### MySQL

(TODO schema support and migration. Use Ecto tooling?)

Setup the various database environment variables as per config/ before proceeding.

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

- Create MySQL database*:

        (TODO database schema setup instructions)

- Create Kafka topic*:

        kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic foo --partitions 1 --replication-factor 1


- Run Integration Tests

        mix test --only integration --no-start


