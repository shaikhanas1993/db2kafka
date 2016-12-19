defmodule Db2Kafka.TopicBuffer do
  require Logger

  defstruct [:num_partitions, :partitions]

  defp genserver_name(topic) do
    String.to_atom("#{__MODULE__}-#{topic}")
  end

  @spec start_link(String.t, non_neg_integer) :: GenServer.on_start
  def start_link(topic, num_partitions) do
    name = genserver_name(topic)
    _ = Logger.info("Starting #{name}")
    GenServer.start_link(__MODULE__, num_partitions, [name: name])
  end

  def init(num_partitions) do
    state = %Db2Kafka.TopicBuffer{num_partitions: num_partitions, partitions: %{}}
    {:ok, state}
  end

  # API
  @spec get_records(String.t, non_neg_integer) :: {:ok, list(Db2Kafka.Record.t)}
  def get_records(topic, partition_id) do
    name = genserver_name(topic)
    GenServer.call(name, {:get_records, topic, partition_id})
  end

  # Server
  def handle_call({:get_records, topic, partition_id}, _from,  %Db2Kafka.TopicBuffer{num_partitions: num_partitions, partitions: old_partitions} = _old_state) do
    partitions = if length(Map.get(old_partitions, partition_id, [])) == 0 do
      records_result = Db2Kafka.RecordBuffer.get_records(topic)

      fetched_partitioned_records = case records_result do
        {:ok, records} ->
          Enum.group_by(records, fn(r) ->
            make_partition_id(r.partition_key, num_partitions)
          end)
        {:error, _msg} ->
          %{}
      end

      merge_partitioned_records(old_partitions, fetched_partitioned_records)
    else
      old_partitions
    end

    %{records: records, partitions: new_partitions} = get_records_from_bucket(partitions, partition_id)

    state = %Db2Kafka.TopicBuffer{num_partitions: num_partitions, partitions: new_partitions}
    {:reply, {:ok, records}, state}
  end

  defp make_partition_id(partition_key, num_partitions) do
    hash = Murmur.hash_x86_32(partition_key)
    rem(hash, num_partitions)
  end

  defp get_records_from_bucket(partitions, partition_id) do
    partition_bucket = Map.get(partitions, partition_id, [])
    case partition_bucket do
      [] ->
        %{records: [], partitions: partitions}
      records ->
        partitions = Map.delete(partitions, partition_id)
        %{records: records, partitions: partitions}
    end
  end

  defp merge_partitioned_records(orig_partitions, new_partitions) do
    Map.merge(orig_partitions, new_partitions, fn(_p_id, orig_records, new_records) ->
      Enum.uniq_by(orig_records ++ new_records, fn(r) -> r.id end)
    end)
  end
end
