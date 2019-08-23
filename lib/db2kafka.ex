defmodule Db2Kafka do
  use Application
  require Logger

  def start(_type \\ nil, _args \\ nil) do
    import Supervisor.Spec
    children = [ 
      supervisor(Failover.Harness, []), 
    ]
    Supervisor.start_link(children, [strategy: :one_for_one])
  end

  def prep_stop(_state) do
    Db2Kafka.AppWatcher.before_stop()
  end

  def stop(_state) do
    Db2Kafka.AppWatcher.after_stop()
  end

end
