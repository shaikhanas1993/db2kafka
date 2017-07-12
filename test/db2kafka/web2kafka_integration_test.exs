defmodule Db2Kafka.IntegrationTest do
  use ExUnit.Case
  import Eventually

  @table "outbound_kafka_queue"

  @moduletag :integration

  @length_known_records 100

  @tag :integration
  setup do
    DatabaseHelper.drop_and_create_table
    known_records = Enum.map(1..@length_known_records, fn(n) ->
      %Db2Kafka.Record{id: n, topic: "foo", partition_key: "123",
        body: "body#{n}", created_at: :os.system_time(:seconds)}
    end)
    unknown_record = [%Db2Kafka.Record{id: 101, topic: "unknown", partition_key: "123",
        body: "body", created_at: :os.system_time(:seconds)}]
    records = Enum.concat(unknown_record, known_records)
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
  test "records for known topics in the DB get published then deleted without duplicates" do
    all_records_query = "select * from #{@table}"
    {:ok, db_pid} = TestHelpers.create_db_pid

    endpoints = Application.get_env(:kaffe, :producer)[:endpoints]

    {:ok, [starting_offset]} = :brod.get_offsets(endpoints, "foo", 0)

    assert_eventually(1_500, fn ->
      case :brod.fetch(endpoints, "foo", 0, starting_offset) do
        {:ok, messages} ->
          length(messages) == @length_known_records
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
      case :brod.fetch(endpoints, "foo", 0, starting_offset + 1) do
        {:ok, messages} ->
          length(messages) == @length_known_records
        _ ->
          false
      end
    end)

    assert_eventually(1_500, fn ->
      assert query_results_length(db_pid, all_records_query) == 0
    end)
  end
end
