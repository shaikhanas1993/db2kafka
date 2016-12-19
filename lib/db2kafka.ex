defmodule Db2Kafka do
  use Application
  require Logger

  def start(_type \\ nil, _args \\ nil) do
    _ = Logger.info("Starting db2kafka")

    {:ok, _} = Db2Kafka.Stats.start_link("db2kafka")
    {:ok, _} = Db2Kafka.Supervisor.start_link
  end
end
