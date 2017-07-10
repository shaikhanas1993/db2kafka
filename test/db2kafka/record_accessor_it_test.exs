defmodule RecordAccessor.IntegrationTest do
  use ExUnit.Case

  setup_all do
    RecordCreator.start_link
    Db2Kafka.RecordAccessor.start_link
    db_username = Application.get_env(:db2kafka, :db_username)
    db_password = Application.get_env(:db2kafka, :db_password)
    db_hostname = Application.get_env(:db2kafka, :db_hostname)
    db_name = Application.get_env(:db2kafka, :db_name)

    {:ok, db_pid} = Mariaex.start_link(
      username: db_username,
      password: db_password,
      hostname: db_hostname,
      database: db_name
    )

    {:ok, db_pid: db_pid}
  end

  setup do
    DatabaseHelper.drop_and_create_table
  end

  def query_results_length(pid, query) do
    case Mariaex.query(pid, query) do
      {:ok, %Mariaex.Result{rows: rows_data}} ->
        length(rows_data)
    end
  end

  @tag :integration
  test "get_min_id returns the lowest id if rows exist", %{db_pid: db_pid} do
    records = Enum.map(42..100, fn(n) ->
      %Db2Kafka.Record{id: n, topic: "foo", partition_key: "123", body: "body#{n}", created_at: :erlang.system_time()}
    end)
    RecordCreator.create_records(records)

    min_id_result = Db2Kafka.RecordAccessor.get_min_id(db_pid)
    assert min_id_result == {:ok, 42}
  end

  @tag :integration
  test "get_min_id returns {:error, :empty_table} if the table is empty", %{db_pid: db_pid} do
    min_id = Db2Kafka.RecordAccessor.get_min_id(db_pid)
    assert min_id == {:error, :empty_table}
  end

  @tag :integration
  test "it gets a batch of records, given a minimum ID and id partition size" do
    base_id = 100
    record_ids = Enum.map(0..99, fn(n) -> base_id + n * 3 end)
    records = Enum.map(record_ids, fn(id) ->
      %Db2Kafka.Record{id: id, topic: "foo", partition_key: "123", body: "body#{id}", created_at: :erlang.system_time()}
    end)
    RecordCreator.create_records(records)

    id_space_partition_size = 42

    assert Db2Kafka.RecordAccessor.get_records_async(self(), id_space_partition_size) == :ok

    receive do
      {:"$gen_cast", {:get_records_result, {:ok, records}}} ->
        returned_record_ids = Enum.map(records, &(&1.id))
        filtered_record_ids = Enum.filter(record_ids, &(&1 < base_id + id_space_partition_size))
        assert returned_record_ids == filtered_record_ids
    end
  end

  @tag :integration
  test "it correctly calculates age from the database" do
    created_at_sec = :erlang.system_time(:seconds)
    RecordCreator.create_records([%Db2Kafka.Record{id: 1, topic: "foo", partition_key: "123", body: "bar",
       created_at: created_at_sec - 60}])

    assert Db2Kafka.RecordAccessor.get_records_async(self(), 100) == :ok

    receive do
      {:"$gen_cast", {:get_records_result, {:ok, [record | _]}}} ->
        assert Db2Kafka.Record.age(record) >= 60
        assert Db2Kafka.Record.age(record) <= 65
    end
  end
end
