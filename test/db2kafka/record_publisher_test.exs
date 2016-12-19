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
    records = [%Db2Kafka.Record{id: 1, created_at: :erlang.system_time},
               %Db2Kafka.Record{id: 2, created_at: :erlang.system_time}]
    topic = "foo"
    partition = 0

    kafka_ex_mock = [
      produce: fn(_, _) -> [%KafkaEx.Protocol.Produce.Response{partitions: [%{error_code: 0, offset: 101, partition: 0}], topic: topic}] end,
      metadata: fn(^topic) -> %MockMetadataResponse{
        topic_metadatas: [%MockTopicMetadataResponse{
          partition_metadatas: [1]
        }]
      } end,
      create_worker: fn(_) -> {:ok, :worker} end
    ]

    topic_buffer_mock = [
      get_records: fn(^topic, _) -> {:ok, records} end
    ]

    {
      :ok,
      records: records,
      partition: partition,
      topic: topic,
      kafka_ex_mock: kafka_ex_mock,
      topic_buffer_mock: topic_buffer_mock
    }
  end

  @tag :unit
  test_with_mock "it publishes records",
      %{records: records, partition: partition, topic: topic, kafka_ex_mock: kafka_ex_mock},
      KafkaEx, [], kafka_ex_mock do
    Db2Kafka.RecordPublisher.handle_call({:publish_records, topic, partition, records}, self, :kafka_pid)
    assert_eventually(fn -> called(KafkaEx.produce(:_, [worker_name: :kafka_pid])) end)
  end
end
