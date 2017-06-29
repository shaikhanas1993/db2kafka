defmodule Db2Kafka.RecordDeleter do
  use GenServer
  require Logger

  @delete_record_metric "db2kafka.delete_record"
  @zero_record_delete_metric "db2kafka.zero_record_delete"
  @records_to_delete_metric "db2kafka.records_to_delete"
  @records_deleted_metric "db2kafka.records_deleted"
  @delete_latency_metric "db2kafka.delete_latency"
  @table "outbound_kafka_queue"
  @delete_batch_size_metric "db2kafka.delete_batch_size"

  @spec start_link([]) :: GenServer.on_start
  def start_link([]) do
    _ = Logger.info("Starting RecordDeleter")
    GenServer.start_link(__MODULE__, [], [])
  end

  def init(_state) do
    db_username = Application.get_env(:db2kafka, :db_username)
    db_password = Application.get_env(:db2kafka, :db_password)
    db_hostname = Application.get_env(:db2kafka, :db_hostname)
    db_name = Application.get_env(:db2kafka, :db_name)

    _ = Logger.info("Starting DB connection...")
    {:ok, db_pid} = Mariaex.start_link(
      username: db_username,
      password: db_password,
      hostname: db_hostname,
      database: db_name
    )
    _ = Logger.info("DB connection started")

    {:ok, db_pid}
  end

  @spec delete_records(list(Db2Kafka.Record.t), boolean) :: atom
  def delete_records(records, sync \\ true) do
    :poolboy.transaction(
      Db2Kafka.Supervisor.deleter_pool_name,
      fn(pid) ->
        if sync do
          GenServer.call(pid, {:delete_records, records})
        else
          GenServer.cast(pid, {:delete_records, records})
        end
      end,
      :infinity
    )
  end

  def handle_cast({:delete_records, records}, db_pid) do
    _ = Logger.info("Deleting #{length(records)} rows asynchronously")
    do_delete_records(records, db_pid)
    {:noreply, db_pid}
  end

  def handle_call({:delete_records, records}, _from, db_pid) do
    result = do_delete_records(records, db_pid)
    {:reply, result, db_pid}
  end

  def do_delete_records(records, db_pid) do
    joined_record_ids = Enum.map(records, fn(record) -> record.id end)
      |> Enum.join(", ")

    Db2Kafka.Stats.incrementCountBy(@records_to_delete_metric, length(records))
    length(records) |> Db2Kafka.Stats.histogram(@delete_batch_size_metric)

    result = Db2Kafka.Stats.timing(@delete_record_metric, fn ->
      Mariaex.query(db_pid, "DELETE FROM #{@table} WHERE id IN (#{joined_record_ids})")
    end)

    deletion_result =
      case result do
        {:ok, %Mariaex.Result{num_rows: 0}} ->
          Db2Kafka.Stats.incrementSuccess(@zero_record_delete_metric)
          _ = Logger.warn("Record deletion affected 0 of #{length(records)} rows, something is fishy")
          :ok
        {:ok, %Mariaex.Result{num_rows: n}} ->
          last_record = Enum.at(records, -1)
          _ = Logger.info("#{n} rows deleted from topic: #{last_record.topic}")
          Db2Kafka.Stats.incrementSuccess(@delete_record_metric)
          Db2Kafka.Stats.incrementCountBy(@records_deleted_metric, n, ["topic:#{last_record.topic}"])
          Db2Kafka.Stats.timer(@delete_latency_metric, Db2Kafka.Record.age(last_record))
          :ok
        {:error, err} ->
          Db2Kafka.Stats.incrementFailure(@delete_record_metric)
          _ = Logger.error("Couldn't delete record: #{inspect err}")
          :error
      end

    :ok = Db2Kafka.RecordBuffer.remove_downstream_records(records)

    deletion_result
  end
end
