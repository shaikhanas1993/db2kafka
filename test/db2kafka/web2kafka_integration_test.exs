defmodule Db2Kafka.IntegrationTest do
  use ExUnit.Case
  import Eventually

  @table "outbound_kafka_queue"

  @moduletag :integration

  @tag :integration
  setup do
    DatabaseHelper.drop_and_create_table
    records = Enum.map(1..100, fn(n) ->
      %Db2Kafka.Record{id: n, topic: "foo", partition_key: "123",
        body: "body#{n}", created_at: :os.system_time(:seconds)}
    end)
    RecordCreator.start_link
    RecordCreator.create_records(records)
    Application.put_env(:db2kafka, :publish_to_kafka, true)
    Application.put_env(:kafka_ex, :disable_default_worker, false)
    TestHelpers.start_apps
    {
      :ok,
      records: records
    }
  end

  def query_results_length(pid, query) do
    case Mariaex.query(pid, query) do
      {:ok, %Mariaex.Result{rows: rows_data}} ->
        length(rows_data)
    end
  end

  @tag :integration
  test "all records in the DB get published then deleted without duplicates", %{records: records} do
    all_records_query = "select * from #{@table}"
    {:ok, db_pid} = TestHelpers.create_db_pid

    starting_offset = KafkaEx.latest_offset("foo", 0)
      |> Enum.at(0)
      |> (fn(response) -> response.partition_offsets end).()
      |> Enum.at(0)
      |> (fn(offsets) -> offsets.offset end).()
      |> Enum.at(0)

    assert_eventually(1_500, fn ->
      case KafkaEx.fetch("foo", 0, offset: starting_offset, auto_commit: false) do
        [%KafkaEx.Protocol.Fetch.Response{partitions: [%{message_set: messages}]}] ->
          length(messages) == length(records)
        _ ->
          false
      end
    end)

    assert_eventually(1_500, fn ->
      assert query_results_length(db_pid, all_records_query) == 0
    end)

    RecordCreator.create_records([%Db2Kafka.Record{id: 0, topic: "foo",
      partition_key: "123", body: "body0", created_at: :os.system_time(:seconds)}])

    assert_eventually(500, fn ->
      case KafkaEx.fetch("foo", 0, offset: starting_offset + 1, auto_commit: false) do
        [%KafkaEx.Protocol.Fetch.Response{partitions: [%{message_set: messages}]}] ->
          length(messages) == length(records)
        _ ->
          false
      end
    end)

    assert_eventually(1_500, fn ->
      assert query_results_length(db_pid, all_records_query) == 0
    end)
  end
end
