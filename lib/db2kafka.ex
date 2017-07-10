defmodule Db2Kafka do
  use Application
  require Logger

  def start(_type \\ nil, _args \\ nil) do
    Failover.Harness.start_link()
  end
end
