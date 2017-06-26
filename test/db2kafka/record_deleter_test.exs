defmodule Db2Kafka.RecordDeleterTest do
  use ExUnit.Case

  import Mock
  import Eventually

  @tag :unit
  test_with_mock "it deletes records by ID", _context,
    Mariaex, [], [query: fn(_, _) -> {:ok, %Mariaex.Result{num_rows: 1}} end,
                  start_link: fn(_) -> {:ok, :db_pid} end] do
    with_mock Db2Kafka.RecordBuffer, [], [remove_downstream_records: fn(_) -> :ok end] do

      records = [%Db2Kafka.Record{id: 40, created_at: :erlang.system_time},
                 %Db2Kafka.Record{id: 42, created_at: :erlang.system_time}]
      {:ok, pid} = Db2Kafka.RecordDeleter.start_link([])
      GenServer.call(pid, {:delete_records, records})

      expected_query = "DELETE FROM outbound_kafka_queue WHERE id IN (40, 42)"
      assert_eventually(fn -> called(Mariaex.query(:_, expected_query)) end)
    end
  end

  @tag :unit
  test_with_mock "it deletes records asynchronously", _context,
    Mariaex, [], [query: fn(_, _) -> {:ok, %Mariaex.Result{num_rows: 1}} end,
                  start_link: fn(_) -> {:ok, :db_pid} end] do
    with_mock Db2Kafka.RecordBuffer, [], [remove_downstream_records: fn(_) -> :ok end] do

      records = [%Db2Kafka.Record{id: 45, created_at: :erlang.system_time},
                 %Db2Kafka.Record{id: 47, created_at: :erlang.system_time}]
      {:ok, pid} = Db2Kafka.RecordDeleter.start_link([])
      GenServer.cast(pid, {:delete_records, records})

      expected_query = "DELETE FROM outbound_kafka_queue WHERE id IN (45, 47)"
      assert_eventually(fn -> called(Mariaex.query(:_, expected_query)) end)
    end
  end
end
