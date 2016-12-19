defmodule Db2Kafka.TopicHealthTracker do
  @moduledoc """
  This module contains a GenServer that can be used to track per-topic health. When writing to Kafka,
  the result of the operation can be sent here through the `report_partition_ok` and `report_partition_error`
  messages (which are processed async). The GenServer keeps track of partitions across a topic, and whenever
  a state is reached that no partitions are reported healthy, the process will kill itself.

  Used in a supervisor hierarchy, this should trigger a reset of the Kafka client which may help in fixing
  the underlying issue by reconnecting, etcetera.
  """

  use GenServer
  require Logger

  @progress_period_s 60

  @type t :: %Db2Kafka.TopicHealthTracker{
    topic: String.t,
    partitions_status: %{
      non_neg_integer => non_neg_integer
    }
  }
  defstruct [:topic, :partitions_status]

  @spec start_link(String.t, non_neg_integer) :: GenServer.on_start
  def start_link(topic, num_partitions) do
    name = genserver_name(topic)
    _ = Logger.info("Starting #{name}")
    GenServer.start_link(__MODULE__, {topic, num_partitions}, [name: name])
  end

  def init({topic, num_partitions}) do
    start_time = current_unix_time
    # start by assuming every partition is OK, to ensure we don't shutdown immediately after the first error
    partitions_status = Map.new(0..(num_partitions - 1), fn(p) -> {p, start_time} end)
    state = %Db2Kafka.TopicHealthTracker{topic: topic, partitions_status: partitions_status}
    {:ok, state}
  end

  # API
  @spec report_partition_ok(String.t, non_neg_integer) :: :ok
  def report_partition_ok(topic, partition) do
    name = genserver_name(topic)
    GenServer.cast(name, {:report_partition_status, partition, current_unix_time})
  end

  @spec report_partition_error(String.t, non_neg_integer) :: :ok
  def report_partition_error(topic, partition) do
    name = genserver_name(topic)
    GenServer.cast(name, {:report_partition_status, partition, :error})
  end

  # Server
  @spec handle_cast({atom, non_neg_integer, :error | non_neg_integer}, Db2Kafka.TopicHealthTracker.t) :: {:noreply, Db2Kafka.TopicHealthTracker.t}
  def handle_cast({:report_partition_status, partition, status}, old_state) do
    if status == :error do
      crash_if_no_progress_being_made(old_state.topic, old_state.partitions_status)
    end

    new_partition_status = case status do
      :ok -> current_unix_time
      s -> s
    end

    new_partitions_status = Map.put(old_state.partitions_status, partition, new_partition_status)

    {:noreply, %Db2Kafka.TopicHealthTracker{ old_state | :partitions_status => new_partitions_status }}
  end

  defp crash_if_no_progress_being_made(topic, partitions_status) do
    progress_partition = partitions_status
      |> Enum.filter(fn({_, status}) -> status != :error end)
      |> Enum.find(fn({_, last_ok_time}) -> (current_unix_time - last_ok_time) < @progress_period_s end)

    if progress_partition == nil do
      _ = Logger.error("No partitions are making progress in topic #{topic} for #{@progress_period_s} seconds... terminating!")
      Process.exit(self(), :no_progress_being_made)
    end
  end

  defp genserver_name(topic) do
    String.to_atom("#{__MODULE__}-#{topic}")
  end

  defp current_unix_time do
    :os.system_time(:seconds)
  end
end
