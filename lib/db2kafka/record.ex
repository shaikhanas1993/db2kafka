defmodule Db2Kafka.Record do
  @type t :: %Db2Kafka.Record{
    id: integer,
    topic: String.t,
    partition_key: String.t,
    body: String.t,
    created_at: non_neg_integer }
  defstruct [:id, :topic, :partition_key, :body, :created_at]

  @spec age(Db2Kafka.Record.t) :: non_neg_integer
  def age(record) do
    :erlang.system_time(:seconds) - record.created_at
  end
end

defimpl String.Chars, for: Db2Kafka.Record do
  def to_string(record) do
    "Record(id: #{record.id} topic: #{record.topic}, partition_key: #{record.partition_key}, body: #{record.body}, created_at: #{record.created_at})"
  end
end
