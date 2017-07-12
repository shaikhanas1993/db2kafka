defmodule Db2Kafka.RecordPublisherTest do
  use ExUnit.Case

  import Mock
  import Eventually

  defmodule MockMetadataResponse do
    defstruct [:topic_metadatas]
  end

  defmodule MockTopicMetadataResponse do
    defstruct [:partition_metadatas]
  end

  setup do
    records = [%Db2Kafka.Record{id: 1, created_at: :erlang.system_time, body: "body1"},
               %Db2Kafka.Record{id: 2, created_at: :erlang.system_time, body: "body2"}]
    records_mapped = [
      {"", "body1"},
      {"", "body2"},
    ]

    topic = "foo"
    partition = 0

    kaffe_producer_mock = [
      produce_sync: fn(_, _, _) -> :ok end
    ]

    topic_buffer_mock = [
      get_records: fn(^topic, _) -> {:ok, records} end
    ]

    {
      :ok,
      records: records,
      records_mapped: records_mapped,
      partition: partition,
      topic: topic,
      kaffe_producer_mock: kaffe_producer_mock,
      topic_buffer_mock: topic_buffer_mock
    }
  end

  @tag :unit
  test_with_mock "it publishes records",
      %{records: records, records_mapped: records_mapped, partition: partition, topic: topic, kaffe_producer_mock: kaffe_producer_mock},
      Kaffe.Producer, [], kaffe_producer_mock do
    Db2Kafka.RecordPublisher.handle_call({:publish_records, topic, partition, records}, self(), :kafka_pid)
    assert_eventually(fn -> called(Kaffe.Producer.produce_sync(topic, partition, records_mapped)) end)
  end
end
