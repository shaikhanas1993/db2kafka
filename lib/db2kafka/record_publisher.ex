defmodule Db2Kafka.RecordPublisher do
  use GenServer
  require Logger

  @publish_records_metric "db2kafka.publish_record"
  @records_published_metric "db2kafka.records_published"
  @publish_latency_metric "db2kafka.publish_latency"
  @publish_batch_size_metric "db2kafka.publish_batch_size"

  @spec start_link([]) :: GenServer.on_start
  def start_link([]) do
    _ = Logger.info("Starting RecordPublisher")
    GenServer.start_link(__MODULE__, :ok, [])
  end

  @spec publish_records(String.t, non_neg_integer, list(Db2Kafka.Record.t)) :: atom
  def publish_records(topic, partition_id, records) do
    :poolboy.transaction(
      Db2Kafka.Supervisor.publisher_pool_name,
      fn(pid) ->
        GenServer.call(pid, {:publish_records, topic, partition_id, records})
      end,
      :infinity
    )
  end

  def handle_call({:publish_records, topic, partition_id, records}, _from, kafka_pid) do
    length(records) |> Db2Kafka.Stats.histogram(@publish_batch_size_metric)
    result = Db2Kafka.Stats.timing(@publish_records_metric, fn ->
      Kaffe.Producer.produce_sync(topic, partition_id, Enum.map(records, fn(r) -> {"", r.body} end))
    end)

    case result do
      :ok ->
        Db2Kafka.Stats.incrementSuccess(@publish_records_metric)
        _ = Logger.info("Published batch of #{length(records)} records to topic #{topic} partition #{partition_id}")
        Db2Kafka.Stats.incrementCountBy(@records_published_metric, length(records), ["topic:#{topic}"])
        Db2Kafka.Stats.timer(@publish_latency_metric, Db2Kafka.Record.age(Enum.at(records, -1)))
        {:reply, :ok, kafka_pid}
      _ ->
        Db2Kafka.Stats.incrementFailure(@publish_records_metric)
        _ = Logger.error("Failed to publish batch of #{length(records)} records to topic #{topic} partition #{partition_id}")
        {:reply, :error, kafka_pid}
    end
  end
end
