defmodule Db2Kafka.RecordAccessor do
  use GenServer
  require Logger

  @type deletion_result :: :ok | :error
  @type records_result :: {:ok, list(%Db2Kafka.Record{})} | {:error, String.t}

  @get_records_metric "db2kafka.get_records"
  @records_fetched_metric "db2kafka.records_fetched"
  @table "outbound_kafka_queue"

  @spec start_link() :: GenServer.on_start
  def start_link do
    _ = Logger.info("Starting #{__MODULE__}")
    GenServer.start_link(__MODULE__, [], [name: __MODULE__])
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

  # API

  @spec get_records_async(pid, integer) :: :ok
  def get_records_async(reply_to, batch_size) do
    GenServer.cast(__MODULE__, {:get_records_async, reply_to, batch_size})
  end

  # Server

  def handle_cast({:get_records_async, reply_to, id_space_partition_size}, db_pid) do
    records_result = case get_min_id(db_pid) do
      {:error, msg} ->
        {:error, msg}
      {:ok, min_id} ->
        get_records(min_id, id_space_partition_size, db_pid)
    end

    GenServer.cast(reply_to, {:get_records_result, records_result})

    {:noreply, db_pid}
  end

  def get_min_id(db_pid) do
    result = Mariaex.query(db_pid, "SELECT MIN(id) FROM #{@table}")
    min_id_result =
      case result do
        {:ok, %Mariaex.Result{rows: [[nil]]}} ->
          {:error, :empty_table}
        {:ok, %Mariaex.Result{rows: [[rows_data]]}} ->
          _ = Logger.info("Min id is #{rows_data}")
          {:ok, rows_data}
        e ->
          message = "Unexpected error: #{inspect e}"
          _ = Logger.error("Can't get the min_id in #{@table} - #{message}")
          {:error, message}
      end
    min_id_result
  end

  def get_records(min_id, id_space_partition_size, db_pid) do
    query = "SELECT id, topic, item_key, item_body, unix_timestamp(created_at) FROM #{@table} " <>
            "WHERE id < #{min_id + id_space_partition_size}"

    result = Db2Kafka.Stats.timing(@get_records_metric, fn ->
      Mariaex.query(db_pid, query)
    end)

    processed_result =
      case result do
        {:ok, %Mariaex.Result{rows: rows}} ->
          Db2Kafka.Stats.incrementSuccess(@get_records_metric)
          Db2Kafka.Stats.incrementCountBy(@records_fetched_metric, length(rows))
          {:ok, Enum.map(rows, fn(row) ->
            row_as_tuple = List.to_tuple(row)
            %Db2Kafka.Record{
              id: elem(row_as_tuple, 0),
              topic: elem(row_as_tuple, 1),
              partition_key: elem(row_as_tuple, 2),
              body: elem(row_as_tuple, 3),
              created_at: elem(row_as_tuple, 4)
            }
          end)}
        {:error, err} ->
          Db2Kafka.Stats.incrementFailure(@get_records_metric)
          {:error, err.mariadb.message}
      end
    processed_result
  end
end
