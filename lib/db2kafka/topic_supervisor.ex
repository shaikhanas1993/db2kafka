defmodule Db2Kafka.TopicSupervisor do
  use Supervisor
  require Logger

  @spec start_link(String.t) :: Supervisor.on_start
  def start_link(topic) do
    name = String.to_atom("#{__MODULE__}-#{topic}")
    _ = Logger.info("Starting #{name}")
    Supervisor.start_link(__MODULE__, topic, [name: name])
  end

  defp get_num_partitions(topic) do
    endpoints = Application.get_env(:kaffe, :producer)[:endpoints]
    {:ok, metadata} = :brod.get_metadata(endpoints, [topic])
    {:kpro_MetadataResponse, _ ,
      [{:kpro_TopicMetadata, _, ^topic, partition_metadata}]} = metadata

    length(partition_metadata)
  end

  def init(topic) do
    num_partitions = get_num_partitions(topic)
    _ = Logger.debug("Topic #{topic} : num_partitions = #{num_partitions}")
    children = [
      worker(Db2Kafka.TopicBuffer, [topic, num_partitions]),
      worker(Db2Kafka.TopicHealthTracker, [topic, num_partitions])
    ]
    children = children ++ Enum.map(0..num_partitions - 1, fn(n) ->
      worker(Db2Kafka.PartitionProcessor, [topic, n], id: n)
    end)

    supervise(children, strategy: :one_for_one, max_restarts: 0)
  end
end
