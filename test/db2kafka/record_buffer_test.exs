defmodule Db2Kafka.RecordBufferTest do
  use ExUnit.Case

  import Mock

  setup do
    {:ok, records: [%Db2Kafka.Record{partition_key: "123"}, %Db2Kafka.Record{partition_key: "123"}] }
  end

  defp make_downstream_ids(records) do
    Enum.reduce(records, MapSet.new(), fn (r, acc) -> MapSet.put(acc, r.id) end)
  end

  @tag :unit
  test "new_state" do
    assert Db2Kafka.RecordBuffer.new_state(%{}) == %Db2Kafka.RecordBuffer{buckets: %{}, downstream_ids: MapSet.new(), fetching_records: false}
  end

  @tag :unit
  test_with_mock ":remove_downstream_records sets downstream_ids, gets more records when it has none",
    Db2Kafka.RecordAccessor, [], [get_records_async: fn(_, _) -> {:ok, []} end] do

    r1 = %Db2Kafka.Record{id: 1, topic: "foo", partition_key: "k-1-f", body: "Abc"}
    r2 = %Db2Kafka.Record{id: 2, topic: "bar", partition_key: "k-2-b", body: "Def"}
    r3 = %Db2Kafka.Record{id: 3, topic: "bar", partition_key: "k-3-b", body: "Ghi"}

    buckets0 = %{}
    downstream_ids0 = make_downstream_ids([r1, r2, r3])

    state0 = %Db2Kafka.RecordBuffer{buckets: buckets0, downstream_ids: downstream_ids0, fetching_records: false}

    {:reply, result, state1} = Db2Kafka.RecordBuffer.handle_call({:remove_downstream_records, [r1, r2, r3]}, nil, state0)

    assert result == :ok

    assert state1 == %Db2Kafka.RecordBuffer{buckets: %{}, downstream_ids: MapSet.new, fetching_records: false}

    {:reply, _, state2} = Db2Kafka.RecordBuffer.handle_call({:get_records, :topic_foo}, self, state1)

    assert called Db2Kafka.RecordAccessor.get_records_async(:_, Db2Kafka.RecordBuffer.max_batch_size)

    assert state2 == %Db2Kafka.RecordBuffer{buckets: %{}, downstream_ids: MapSet.new, fetching_records: true}
  end

  @tag :unit
  test "records_to_ordered_topic_buckets should group by topic, ordered by id" do

    records = [
      %Db2Kafka.Record{id: 1, topic: "foo", partition_key: "k-1-f", body: "Abc"},
      %Db2Kafka.Record{id: 4, topic: "foo", partition_key: "k-4-f", body: "Abc"},
      %Db2Kafka.Record{id: 3, topic: "bar", partition_key: "k-3-b", body: "Abc"},
      %Db2Kafka.Record{id: 2, topic: "bar", partition_key: "k-2-b", body: "Abc"},
      %Db2Kafka.Record{id: 6, topic: "foo", partition_key: "k-6-f", body: "Abc"},
      %Db2Kafka.Record{id: 5, topic: "baz", partition_key: "k-5-z", body: "Abc"}
    ]

    buckets = Db2Kafka.RecordBuffer.records_to_ordered_topic_buckets(records)

    expected_buckets = %{
      "foo" => [
        %Db2Kafka.Record{id: 1, topic: "foo", partition_key: "k-1-f", body: "Abc"},
        %Db2Kafka.Record{id: 4, topic: "foo", partition_key: "k-4-f", body: "Abc"},
        %Db2Kafka.Record{id: 6, topic: "foo", partition_key: "k-6-f", body: "Abc"},
      ],
      "bar" => [
        %Db2Kafka.Record{id: 2, topic: "bar", partition_key: "k-2-b", body: "Abc"},
        %Db2Kafka.Record{id: 3, topic: "bar", partition_key: "k-3-b", body: "Abc"},
      ],
      "baz" => [
        %Db2Kafka.Record{id: 5, topic: "baz", partition_key: "k-5-z", body: "Abc"}
      ],
    }

    assert buckets == expected_buckets
  end

  @tag :unit
  test "num_entries_in_buckets should count records correctly" do
    assert Db2Kafka.RecordBuffer.num_entries_in_buckets(%{}) == 0
    assert Db2Kafka.RecordBuffer.num_entries_in_buckets(%{"foo" => [], "bar" => []}) == 0
    assert Db2Kafka.RecordBuffer.num_entries_in_buckets(%{"foo" => [:a, :b], "baz" => [:c]}) == 3
    assert Db2Kafka.RecordBuffer.num_entries_in_buckets(%{"foo" => [:a], "bar" => [], "baz" => [:b, :c, :d], "oof" => [:e]}) == 5
  end
end
