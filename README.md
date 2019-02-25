# Db2Kafka

[![Build Status](https://travis-ci.org/PagerDuty/db2kafka.svg?branch=master)](https://travis-ci.org/PagerDuty/db2kafka)

This service takes records from a database's `outbound_kafka_queue` table and
ships them to Kafka. We developed it to solve an in-house problem: how do you
move data into Kafka while keeping our Rails app's transactional boundaries?

We are aware that this is a bit of a hack - using tables as queues always ends
up in tears. However, it is a quick way to write data from Rails within the
scope of a MySQL transaction, it is lighter weight and arguably more flexible
than a binlog-based solution, and it works for us.

Hopefully it works for you, too ;-)

Note that documentation is mildly lacking at the moment, probably. We've been running
this thing for a while, so it's all logical and straightforward to us. Feel free to raise
issues if there's something specific missing so we can hone our efforts.


## Local web2kafka

Setup:
```sh
git clone git@github.com:PagerDuty/db2kafka.git && cd db2kafka
git checkout AE-245-web2kafka
asdf install
mix deps.get
```
Run:
```sh
DB_TABLE=pipe_to_localpipe TOPICS=escalation_policy_updates,user_updates,permissions,team_hierarchy BARRIER_PATH=/db2kafka_failover_barrier_localpipe iex -S mix &
DB_TABLE=pipe_to_bitpipe TOPICS=team_updates,entitlements BARRIER_PATH=/db2kafka_failover_barrier_bitpipe  iex -S mix &
```
or
```sh
./web2kafka.sh
```

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

## License

This software is licensed under the MIT license:

The MIT License (MIT)
Copyright (c) 2016 PagerDuty, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
