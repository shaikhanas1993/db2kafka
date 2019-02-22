#!/bin/bash
DB_TABLE=pipe_to_localpipe TOPICS=escalation_policy_updates,user_updates,permissions,team_hierarchy BARRIER_PATH=/db2kafka_failover_barrier_localpipe iex -S mix &
localpipe=$!
DB_TABLE=pipe_to_bitpipe TOPICS=team_updates,entitlements BARRIER_PATH=/db2kafka_failover_barrier_bitpipe iex -S mix &
bitpipe=$!
wait $localpipe $bitpipe
