#!/bin/bash
DB_TABLE=pipe_to_localpipe TOPICS=escalation_policy_updates,user_updates,permissions,team_hierarchy,incidents_by_account,incident_notification_updates BARRIER_PATH=/db2kafka_failover_barrier_localpipe iex -S mix &
localpipe=$!
DB_TABLE=pipe_to_bitpipe TOPICS=team_updates,entitlements,ile_group_requests,incidents_by_account,ihm_messages BARRIER_PATH=/db2kafka_failover_barrier_bitpipe iex -S mix &
bitpipe=$!
wait $localpipe $bitpipe
