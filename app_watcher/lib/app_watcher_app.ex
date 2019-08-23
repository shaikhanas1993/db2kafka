defmodule Db2Kafka.AppWatcher.Application do
  use Application
  require Logger

  def start(_type \\ nil, _args \\ nil) do
    import Supervisor.Spec
    children = [ 
      supervisor(Db2Kafka.AppWatcher, []), 
    ]
    Supervisor.start_link(children, [strategy: :one_for_one])
  end

  def stop(_state) do
    System.halt(0)
  end

end
