defmodule Db2Kafka.TopicBufferTest do
  use ExUnit.Case

  import Mock

  @tag :unit
  test "initial state is correct" do
    num_partitions = 64
    {:ok, state} = Db2Kafka.TopicBuffer.init(64)
    assert state == %Db2Kafka.TopicBuffer{num_partitions: num_partitions, partitions: %{}}
  end

  @tag :unit
  test "records are correctly given out for a partition" do
    partitions = %{
      0 => [
        %Db2Kafka.Record{id: 1, topic: "foo", partition_key: "0"},
        %Db2Kafka.Record{id: 2, topic: "foo", partition_key: "0"}
      ],
      1 => [
        %Db2Kafka.Record{id: 3, topic: "foo", partition_key: "1"},
        %Db2Kafka.Record{id: 4, topic: "foo", partition_key: "1"},
        %Db2Kafka.Record{id: 5, topic: "foo", partition_key: "1"}
      ],
      2 => [
        %Db2Kafka.Record{id: 6, topic: "foo", partition_key: "2"}
      ]
    }

    old_state = %Db2Kafka.TopicBuffer{num_partitions: 3, partitions: partitions}

    {:reply, {:ok, records}, new_state} = Db2Kafka.TopicBuffer.handle_call({:get_records, "foo", 2}, self(), old_state)

    assert [%Db2Kafka.Record{id: 6, topic: "foo", partition_key: "2"}] == records
    assert new_state.num_partitions == 3
    assert new_state.partitions == %{
      0 => [
        %Db2Kafka.Record{id: 1, topic: "foo", partition_key: "0"},
        %Db2Kafka.Record{id: 2, topic: "foo", partition_key: "0"}
      ],
      1 => [
        %Db2Kafka.Record{id: 3, topic: "foo", partition_key: "1"},
        %Db2Kafka.Record{id: 4, topic: "foo", partition_key: "1"},
        %Db2Kafka.Record{id: 5, topic: "foo", partition_key: "1"}
      ]
    }
  end

  @tag :unit
  test "it fetches records from the RecordBuffer when a requested partition is empty" do
    records = [
      %Db2Kafka.Record{id: 1, topic: "foo", partition_key: "0"}, # partition_id == 0
      %Db2Kafka.Record{id: 2, topic: "foo", partition_key: "0"},
      %Db2Kafka.Record{id: 4, topic: "foo", partition_key: "13"}, # partition_id == 1
      %Db2Kafka.Record{id: 5, topic: "foo", partition_key: "13"},
      %Db2Kafka.Record{id: 200, topic: "foo", partition_key: "13"},
      %Db2Kafka.Record{id: 6, topic: "foo", partition_key: "22"} # partition_id == 2
    ]

    with_mock Db2Kafka.RecordBuffer, [get_records: fn(_) -> {:ok, records} end] do
      old_state = %Db2Kafka.TopicBuffer{
        num_partitions: 3,
        partitions: %{
          1 => [
            %Db2Kafka.Record{id: 3, topic: "foo", partition_key: "13"},
            %Db2Kafka.Record{id: 4, topic: "foo", partition_key: "13"},
            %Db2Kafka.Record{id: 5, topic: "foo", partition_key: "13"}
          ]
        }
      }

      {:reply, {:ok, records}, new_state} = Db2Kafka.TopicBuffer.handle_call({:get_records, "foo", 2}, self(), old_state)

      assert records == [
        %Db2Kafka.Record{id: 6, topic: "foo", partition_key: "22"}
      ]
      assert new_state.num_partitions == 3

      assert new_state.partitions[0] == [
        %Db2Kafka.Record{id: 1, topic: "foo", partition_key: "0"},
        %Db2Kafka.Record{id: 2, topic: "foo", partition_key: "0"}
      ]

      assert new_state.partitions[1] == [
        %Db2Kafka.Record{id: 3, topic: "foo", partition_key: "13"},
        %Db2Kafka.Record{id: 4, topic: "foo", partition_key: "13"},
        %Db2Kafka.Record{id: 5, topic: "foo", partition_key: "13"},
        %Db2Kafka.Record{id: 200, topic: "foo", partition_key: "13"}
      ]

      assert new_state.partitions[2] == nil
    end
  end
end
