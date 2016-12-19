defmodule Db2Kafka.TopicHealthTracker.Test do
  use ExUnit.Case

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  def assert_terminated do
    flag = receive do
      {:EXIT, _, _} -> true
      _ -> false
    after
      50 -> false
    end
    assert flag
  end

  def assert_not_terminated do
    flag = receive do
      {:EXIT, _, _} -> false
    after
      50 -> true
    end
    assert flag
  end

  def make_state(topic, partitions_status) do
    %Db2Kafka.TopicHealthTracker{topic: topic, partitions_status: partitions_status}
  end

  @tag :unit
  test "it initializes state correctly" do
    topic = "init_test"
    num_partitions = 3

    {:ok, generated_state} = Db2Kafka.TopicHealthTracker.init({topic, num_partitions})

    assert generated_state.topic == topic
    status = generated_state.partitions_status
    assert length(Map.keys(status)) == num_partitions
    assert status[0] == status[1]
    assert status[1] == status[2]
    assert abs(status[0] - current_unix_time) < 60 # statuses are Unix timestamps within a minute of now
  end

  @tag :unit
  test "it terminates if it receives a partition error and no partitions are making progress" do
    topic = "crash_topic"
    old_state = make_state(topic, %{0 => current_unix_time - 61, 1 => :error})

    Db2Kafka.TopicHealthTracker.handle_cast({:report_partition_status, 0, :error}, old_state)

    assert_terminated
  end

  @tag :unit
  test "it does not terminate if it receives a partition error but other partitions are making progress" do
    topic = "topic"
    part_1_time = current_unix_time
    old_state = make_state(topic, %{0 => current_unix_time - 61, 1 => part_1_time})

    assert Db2Kafka.TopicHealthTracker.handle_cast({:report_partition_status, 0, :error}, old_state) ==
      {:noreply, %Db2Kafka.TopicHealthTracker{ old_state | :partitions_status => %{0 => :error, 1 => part_1_time} }}

    assert_not_terminated
  end

  @tag :unit
  test "it updates state when it receives a partition OK" do
    topic = "topic"
    part_1_time = current_unix_time
    old_state = make_state(topic, %{0 => current_unix_time - 61, 1 => part_1_time})

    response = Db2Kafka.TopicHealthTracker.handle_cast({:report_partition_status, 0, :ok}, old_state)

    assert elem(response, 0) == :noreply
    state = elem(response, 1)
    assert state.topic == topic
    assert state.partitions_status[1] == part_1_time
    assert abs(state.partitions_status[0] - current_unix_time) < 30

    assert_not_terminated
  end

  defp current_unix_time do
    DateTime.to_unix(DateTime.utc_now)
  end
end
