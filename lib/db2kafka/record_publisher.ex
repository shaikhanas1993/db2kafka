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

  def init(:ok) do
    GenServer.start_link(KafkaEx.Server,
      [
        [uris: Application.get_env(:kafka_ex, :brokers),
         consumer_group: Application.get_env(:kafka_ex, :consumer_group)],
        :no_name
      ]
    )
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
      produce_request = %KafkaEx.Protocol.Produce.Request{
        topic: topic,
        partition: partition_id,
        required_acks: -1,
        timeout: 3_000,
        compression: :none,
        messages: Enum.map(records, fn(r) -> %KafkaEx.Protocol.Produce.Message{key: "", value: r.body} end)
      }
      KafkaEx.produce(produce_request, [worker_name: kafka_pid])
    end)

    case result do
     [%KafkaEx.Protocol.Produce.Response{partitions: [%{error_code: 0}]}] ->
        Db2Kafka.Stats.incrementSuccess(@publish_records_metric)
        _ = Logger.info("Published batch of #{length(records)} records to topic #{topic} partition #{partition_id}")
        Db2Kafka.Stats.incrementCountBy(@records_published_metric, length(records), ["topic:#{topic}"])
        age = Db2Kafka.Record.age(Enum.at(records, -1))
        Db2Kafka.Stats.timer(@publish_latency_metric, age)
        if age > Application.get_env(:db2kafka, :publish_latency_sla_95percentile) do
          Db2Kafka.Stats.incrementCountBy(@publish_latency_records_over_95percentile_metric, length(records), ["topic:#{topic}"])
          if age > Application.get_env(:db2kafka, :publish_latency_sla_max) do
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
