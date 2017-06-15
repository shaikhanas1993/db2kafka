defmodule Db2Kafka.RecordBuffer do
  use GenServer
  require Logger

  defstruct buckets: %{}, downstream_ids: %MapSet{}, fetching_records: false

  @max_batch_size 3_000

  @records_served_metric "db2kafka.buffer.records_served"
  @duplicates_metric "db2kafka.buffer.duplicates"

  @type record_result :: {:ok, [%Db2Kafka.Record{}]} | {:error, String.t | :no_record}

  @spec start_link(map) :: GenServer.on_start
  def start_link(buckets \\ %{}) do
    _ = Logger.info("Starting RecordBuffer")
    state = new_state(buckets)
    GenServer.start_link(__MODULE__, state, [name: __MODULE__])
  end

  def new_state(buckets) do
    %Db2Kafka.RecordBuffer{buckets: buckets, downstream_ids: MapSet.new, fetching_records: false}
  end

  def max_batch_size, do: @max_batch_size

  # API

  @spec get_records(number) :: record_result
  def get_records(topic) do
    GenServer.call(__MODULE__, {:get_records, topic})
  end

  def remove_downstream_records(records) do
    GenServer.call(__MODULE__, {:remove_downstream_records, records})
  end

  # Server

  # Remove records that have finished the trip downstream
  def handle_call({:remove_downstream_records, records}, _from, old_state) do
    %Db2Kafka.RecordBuffer{buckets: buckets, downstream_ids: downstream_ids, fetching_records: fetching_records} = old_state

    record_ids = Enum.map(records, &(&1.id))
    downstream_ids = Enum.reduce(record_ids, downstream_ids, fn(r_id, d_ids) ->
      MapSet.delete(d_ids, r_id)
    end)

    _ = Logger.info("There are #{MapSet.size(downstream_ids)} downstream ids")

    state = %Db2Kafka.RecordBuffer{buckets: buckets, downstream_ids: downstream_ids, fetching_records: fetching_records}
    {:reply, :ok, state}
  end

  def handle_call({:get_records, _topic}, _from, %Db2Kafka.RecordBuffer{fetching_records: true} = state) do
    {:reply, {:ok, []}, state}
  end

  def handle_call({:get_records, _topic}, _from, %Db2Kafka.RecordBuffer{buckets: buckets, downstream_ids: downstream_ids, fetching_records: false} = state) when
      buckets == %{} and downstream_ids == %MapSet{} do
    Db2Kafka.RecordAccessor.get_records_async(self(), @max_batch_size)
    new_state = Map.put(state, :fetching_records, true)
    {:reply, {:ok, []}, new_state}
  end

  def handle_call({:get_records, topic}, _from, old_state) do
    %Db2Kafka.RecordBuffer{buckets: buckets, downstream_ids: downstream_ids} = old_state

    topic_bucket = Map.get(buckets, topic, [])
    {new_buckets, to_return} =
      case topic_bucket do
        [] ->
          {buckets, {:error, :no_record}}
        records ->
          orig_len = num_entries_in_buckets(buckets)
          buckets = Map.delete(buckets, topic)
          new_len = num_entries_in_buckets(buckets)

          od_len = MapSet.size(downstream_ids)

          send_len = length(records)
          :ok = if send_len > 0 do
            Db2Kafka.Stats.incrementCountBy(@records_served_metric, send_len, ["topic:#{topic}"])
            _ = Logger.debug(":get_records > topic #{topic} : serving up #{send_len} records ; buffer: #{orig_len} -> #{new_len} ; downstream_ids: #{od_len} -> #{MapSet.size(downstream_ids)}")
          end

          _ = Logger.debug("Sending back #{length(records)} records")
          {buckets, {:ok, records}}
      end

    state = %Db2Kafka.RecordBuffer{buckets: new_buckets, downstream_ids: downstream_ids, fetching_records: false}
    {:reply, to_return, state}
  end

  def handle_cast({:get_records_result, {:ok, records}}, _) do
    # Track records that are being sent downstream
    downstream_ids = Enum.reduce(records, %MapSet{}, fn(r, d_ids) ->
      MapSet.put(d_ids, r.id)
    end)

    known_topics = Application.get_env(:db2kafka, :topics)
    {records_in_known_topics, records_to_delete} = Enum.split_with(records, fn(r) -> Enum.member?(known_topics, r.topic) end)

    Db2Kafka.RecordDeleter.delete_records(records_to_delete)

    buckets = records_to_ordered_topic_buckets(records_in_known_topics)

    {:noreply, %Db2Kafka.RecordBuffer{buckets: buckets, downstream_ids: downstream_ids, fetching_records: false}}
  end
  def handle_cast({:get_records_result, {:error, :empty_table}}, _) do
    {:noreply, %Db2Kafka.RecordBuffer{}}
  end
  def handle_cast({:get_records_result, {:error, msg}}, _) do
     _ = Logger.error(msg)
    {:noreply, %Db2Kafka.RecordBuffer{}}
  end

  def records_to_ordered_topic_buckets(records) do
    Enum.group_by(records, fn(r) -> r.topic end)
      |> Enum.reduce(%{}, fn ({topic, unsorted_records}, buckets) ->
        sorted_records = Enum.sort_by(unsorted_records, fn(r) -> r.id end)
        Map.put(buckets, topic, sorted_records)
      end)
  end

  def num_entries_in_buckets(buckets) do
    values = Map.values(buckets)
    lengths = Enum.map(values, fn(records) -> length(records) end)
    Enum.sum(lengths)
  end
end
