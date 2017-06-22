defmodule Db2Kafka.PartitionProcessor.Test do
  use ExUnit.Case

  import Mock

  def assert_rescheduled do
    flag = receive do
      :publish_and_delete -> true
      _ -> false
    after
      50 -> false
    end
    assert flag
  end

  def make_state(topic, partition_id, records_to_retry \\ [], try_count \\ 0) do
    %Db2Kafka.PartitionProcessor{topic: topic, partition_id: partition_id, records_to_retry: records_to_retry, try_count: try_count}
  end



  @tag :unit
  test "it initializes state correctly" do
    topic = "foo"
    partition_id = 42
    expected_state = make_state(topic, partition_id)
    assert Db2Kafka.PartitionProcessor.init([topic, partition_id]) == {:ok, expected_state}
  end

  @tag :unit
  test "it sends itself a message to start getting records" do
    Db2Kafka.PartitionProcessor.init(["foo", 42])
    assert_rescheduled()
  end

  @tag :unit
  test_with_mock "if a publish is successful and publishing enabled, we delete records",
    Db2Kafka.TopicBuffer, [], [get_records: fn(_, _) -> {:ok, [1]} end] do
    with_mock Db2Kafka.RecordPublisher, [], [publish_records: fn(_, _, _) -> :ok end] do
    with_mock Db2Kafka.RecordDeleter, [], [delete_records: fn(_) -> :ok end] do

      Application.put_env(:db2kafka, :publish_to_kafka, true)
      topic = "foo"
      partition_id = 42
      state = make_state(topic, partition_id)
      Db2Kafka.PartitionProcessor.handle_info(:publish_and_delete, state)

      assert called Db2Kafka.RecordPublisher.publish_records(topic, partition_id, [1])
      assert called Db2Kafka.RecordDeleter.delete_records([1])
      assert_rescheduled()
    end
    end
  end

  @tag :unit
  test_with_mock "if a publish fails, it's retried and records are not deleted",
    Db2Kafka.TopicBuffer, [], [get_records: fn(_, _) -> {:ok, ["retry test"]} end] do
    with_mock Db2Kafka.RecordPublisher, [], [publish_records: fn(_, _, _) -> :error end] do
    with_mock Db2Kafka.RecordDeleter, [], [delete_records: fn(_) -> :ok end] do

      Application.put_env(:db2kafka, :publish_to_kafka, true)
      topic = "foo"
      partition_id = 42
      state = make_state(topic, partition_id)
      Db2Kafka.PartitionProcessor.handle_info(:publish_and_delete, state)

      assert called Db2Kafka.RecordPublisher.publish_records(topic, partition_id, ["retry test"])
      refute called Db2Kafka.RecordDeleter.delete_records(["retry test"])
      assert_rescheduled()
      assert called Db2Kafka.RecordPublisher.publish_records(topic, partition_id, ["retry test"])
    end
    end
  end

  @tag :unit
  test_with_mock "if a publish is being retried, new records are not fetched",
    Db2Kafka.TopicBuffer, [], [get_records: fn(_, _) -> {:ok, []} end] do
    with_mock Db2Kafka.RecordPublisher, [], [publish_records: fn(_, _, _) -> :ok end] do
    with_mock Db2Kafka.RecordDeleter, [], [delete_records: fn(_) -> :ok end] do

      Application.put_env(:db2kafka, :publish_to_kafka, true)
      topic = "foo"
      partition_id = 42
      records_to_retry = ["some retry"]
      state = make_state(topic, partition_id, records_to_retry, 1)
      Db2Kafka.PartitionProcessor.handle_info(:publish_and_delete, state)

      refute called Db2Kafka.TopicBuffer.get_records
      assert called Db2Kafka.RecordPublisher.publish_records(topic, partition_id, records_to_retry)
      assert called Db2Kafka.RecordDeleter.delete_records(records_to_retry)
      assert_rescheduled()
    end
    end
  end

  @tag :unit
  test_with_mock "if publishing is not turned on, we still delete records",
    Db2Kafka.TopicBuffer, [], [get_records: fn(_, _) -> {:ok, [1]} end] do
    with_mock Db2Kafka.RecordPublisher, [], [publish_records: fn(_, _, _) -> :ok end] do
    with_mock Db2Kafka.RecordDeleter, [], [delete_records: fn(_) -> :ok end] do

      Application.put_env(:db2kafka, :publish_to_kafka, false)
      topic = "foo"
      partition_id = 42
      state = make_state(topic, partition_id)
      Db2Kafka.PartitionProcessor.handle_info(:publish_and_delete, state)

      refute called Db2Kafka.RecordPublisher.publish_records(topic, partition_id, [1])
      assert called Db2Kafka.RecordDeleter.delete_records([1])
      assert_rescheduled()
    end
    end
  end

  @tag :unit
  test_with_mock "if we get no records, we do nothing",
    Db2Kafka.TopicBuffer, [], [get_records: fn(_, _) -> {:ok, []} end] do
    with_mock Db2Kafka.RecordPublisher, [], [publish_records: fn(_, _, _) -> :ok end] do
    with_mock Db2Kafka.RecordDeleter, [], [delete_records: fn(_) -> :ok end] do

      Application.put_env(:db2kafka, :publish_to_kafka, true)
      topic = "foo"
      partition_id = 42
      state = make_state(topic, partition_id)
      Db2Kafka.PartitionProcessor.handle_info(:publish_and_delete, state)

      refute called Db2Kafka.RecordPublisher.publish_records(topic, partition_id, [])
      refute called Db2Kafka.RecordDeleter.delete_records([])
      assert_rescheduled()
    end
    end
  end

  @tag :unit
  test_with_mock "if we fail to publish, we don't delete",
    Db2Kafka.TopicBuffer, [], [get_records: fn(_, _) -> {:ok, [1]} end] do
    with_mock Db2Kafka.RecordPublisher, [], [publish_records: fn(_, _, _) -> :error end] do
    with_mock Db2Kafka.RecordDeleter, [], [delete_records: fn(_) -> :ok end] do

      Application.put_env(:db2kafka, :publish_to_kafka, true)
      topic = "foo"
      partition_id = 42
      state = make_state(topic, partition_id)
      try do
        Db2Kafka.PartitionProcessor.handle_info(:publish_and_delete, state)
      rescue
        _error in MatchError ->
          assert called Db2Kafka.RecordPublisher.publish_records(topic, partition_id, [1])
          refute called Db2Kafka.RecordDeleter.delete_records(:_)
      end
    end
    end
  end
end
