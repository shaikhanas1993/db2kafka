defmodule Db2Kafka.RecordPublisher do
  use GenServer
  require Logger

  @publish_records_metric "db2kafka.publish_record"
  @records_published_metric "db2kafka.records_published"
  @publish_latency_metric "db2kafka.publish_latency"
  @publish_batch_size_metric "db2kafka.publish_batch_size"
  @publish_latency_records_over_95percentile_metric "db2kafka.publish_latency_records_over_95percentile"
  @publish_latency_records_over_max_metric "db2kafka.publish_latency_records_over_max"

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
        publish_latency_seconds = Db2Kafka.Record.age(Enum.at(records, -1))
        Db2Kafka.Stats.timer(@publish_latency_metric, publish_latency_seconds)
        if publish_latency_seconds * 1000 > Application.get_env(:db2kafka, :publish_latency_sla_95perc_threshold_ms) do
          Db2Kafka.Stats.incrementCountBy(@publish_latency_records_over_95percentile_metric, length(records), ["topic:#{topic}"])
          if publish_latency_seconds * 1000 > Application.get_env(:db2kafka, :publish_latency_sla_max_threshold_ms) do
            Db2Kafka.Stats.incrementCountBy(@publish_latency_records_over_max_metric, length(records), ["topic:#{topic}"])
          end
        end
        {:reply, :ok, kafka_pid}
      _ ->
        Db2Kafka.Stats.incrementFailure(@publish_records_metric)
        _ = Logger.error("Failed to publish batch of #{length(records)} records to topic #{topic} partition #{partition_id}")
        {:reply, :error, kafka_pid}
    end
  end
end
