defmodule Db2Kafka.PartitionProcessor do
  use GenServer
  use Bitwise
  require Logger

  @default_delay_ms 10
  @max_delay_ms 10_000

  defstruct [:topic, :partition_id, :records_to_retry, :try_count]

  @spec start_link(String.t, non_neg_integer) :: GenServer.on_start
  def start_link(topic, partition_id) do
    name = "#{__MODULE__}-#{topic}-#{partition_id}"
    _ = Logger.info("Starting #{name}")
    GenServer.start_link(__MODULE__, [topic, partition_id], [name: String.to_atom(name)])
  end

  def init([topic, partition_id]) do
    state = %Db2Kafka.PartitionProcessor{topic: topic, partition_id: partition_id, records_to_retry: [], try_count: 0}
    schedule_publish_and_delete(@default_delay_ms)
    {:ok, state}
  end

  # Server

  def handle_info(:publish_and_delete, state) do
    records_to_publish = case state.records_to_retry do
      [] ->
        case Db2Kafka.TopicBuffer.get_records(state.topic, state.partition_id) do
          {:ok, records} when length(records) > 0 ->
            _ = Logger.debug("Got #{length(records)} records for #{state.topic}-#{state.partition_id}")
            records
          _ ->
            []
        end
      records ->
        _ = Logger.debug("Retrying #{length(records)} records for #{state.topic}-#{state.partition_id}")
        records
    end

    records_to_retry = case records_to_publish do
      [] ->
        []
      records ->
        if Application.get_env(:db2kafka, :publish_to_kafka) do
          case Db2Kafka.RecordPublisher.publish_records(state.topic, state.partition_id, records) do
            :ok ->
              Db2Kafka.TopicHealthTracker.report_partition_ok(state.topic, state.partition_id)
              Db2Kafka.RecordDeleter.delete_records(records)
              []
            _ ->
              Db2Kafka.TopicHealthTracker.report_partition_error(state.topic, state.partition_id)
              records
          end
        else
          Db2Kafka.RecordDeleter.delete_records(records)
          []
        end
    end

    new_try_count = case records_to_retry do
      [] -> 0
      _ -> state.try_count + 1
    end

    delay = exponential_delay(new_try_count)

    schedule_publish_and_delete(delay)

    {:noreply, %{state | records_to_retry: records_to_retry, try_count: new_try_count}}
  end

  defp exponential_delay(try_count) do
    min(@default_delay_ms <<< try_count, @max_delay_ms)
  end

  defp schedule_publish_and_delete(delay) do
    Process.send_after(self(), :publish_and_delete, delay)
  end
end
