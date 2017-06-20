defmodule Failover.Harness do
  use GenServer
  require Logger
  
  def start_link() do
    GenServer.start_link(__MODULE__, {})
  end

  def init({}) do
    {:ok, _} = Failover.start_link(self())
    {:ok, {}}
  end
  
  def handle_call(:failover_start, _from, state) do
    Logger.info("Recieved start!")

    Logger.info("Starting db2kafka")
    Db2Kafka.Stats.start_link("db2kafka")
    Db2Kafka.Supervisor.start_link()
    
    {:reply, :ok, state}
  end

  def handle_call(:failover_stop, _from, state) do
    Logger.info("Recieved stop!")
    {:stop, :shutdown, state}
  end
end
