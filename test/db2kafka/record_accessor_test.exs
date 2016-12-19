defmodule Db2Kafka.RecordAccessorTest do
  use ExUnit.Case

  import Mock

  @tag :unit
  test_with_mock "it finds the min record ID",
    Mariaex, [], [query: fn(_, _) -> {:ok, %Mariaex.Result{rows: [[4]]}} end,
                  start_link: fn(_) -> {:ok, :db_pid} end] do

    {:ok, pid} = Db2Kafka.RecordAccessor.start_link
    assert Db2Kafka.RecordAccessor.get_min_id(pid) == {:ok, 4}
    GenServer.stop(pid)
  end

  @tag :unit
  test_with_mock "it finds the min record ID when the table is empty",
    Mariaex, [], [query: fn(_, _) -> {:ok, %Mariaex.Result{rows: [[nil]]}} end,
                  start_link: fn(_) -> {:ok, :db_pid} end] do

    {:ok, pid} = Db2Kafka.RecordAccessor.start_link
    assert Db2Kafka.RecordAccessor.get_min_id(pid)  == {:error, :empty_table}
    GenServer.stop(pid)
  end

  @tag :unit
  test "it gets a batch of records, given a minimum ID" do
    test_id = 8
    test_topic = "some-topic"
    test_partition_key = "some-partition-key"
    test_body = "some-body"
    test_ts = :erlang.system_time()

    with_mock Mariaex, [query: fn(_, _) ->
      {:ok, %Mariaex.Result{columns: ["id", "topic", "item_key", "item_body", "created_at"],
        command: :select,
        connection_id: nil,
        last_insert_id: nil,
        num_rows: 1,
        rows: [
          [test_id, test_topic, test_partition_key, test_body, test_ts]
        ]}} end,
      start_link: fn(_) -> {:ok, :db_pid} end] do

      min_id = 5
      id_space_partition_size = 5
      expected_records = [%Db2Kafka.Record{
        id: test_id,
        topic: test_topic,
        partition_key: test_partition_key,
        body: test_body,
        created_at: test_ts
      }]

      {:ok, pid} = Db2Kafka.RecordAccessor.start_link
      assert Db2Kafka.RecordAccessor.get_records(min_id, id_space_partition_size, pid) == {:ok, expected_records}
    end
  end
end
