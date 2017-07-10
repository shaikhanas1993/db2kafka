defmodule Db2Kafka.Supervisor do
  use Supervisor
  require Logger

  @spec start_link :: Supervisor.on_start
  def start_link do
    _ = Logger.info("Starting supervisor...")
    Supervisor.start_link(__MODULE__, :ok, [name: __MODULE__])
  end

  @spec deleter_pool_name() :: :deleter_pool
  def deleter_pool_name do
    :deleter_pool
  end

  @spec publisher_pool_name() :: :publisher_pool
  def publisher_pool_name do
    :publisher_pool
  end

  def init(:ok) do
    topics = Application.get_env(:db2kafka, :topics)

    children = [worker(Db2Kafka.RecordAccessor, [])]
    children = children ++ [worker(Db2Kafka.RecordBuffer, [])]

    deleter_pool_config = [
      {:name, {:local, deleter_pool_name()}},
      {:worker_module, Db2Kafka.RecordDeleter},
      {:size, Application.get_env(:db2kafka, :deleter_pool_size)},
      {:max_overflow, 0}
    ]

    publisher_pool_config = [
      {:name, {:local, publisher_pool_name()}},
      {:worker_module, Db2Kafka.RecordPublisher},
      {:size, Application.get_env(:db2kafka, :publisher_pool_size)},
      {:max_overflow, 5}
    ]

    children = children ++ [
      :poolboy.child_spec(deleter_pool_name(), deleter_pool_config, [])
    ]

    children = children ++ [
      :poolboy.child_spec(publisher_pool_name(), publisher_pool_config, [])
    ]

    children = children ++ Enum.map(topics, fn(topic) ->
      worker(Db2Kafka.TopicSupervisor, [topic], id: topic)
    end)

    # if the application restarts twice inside 200 seconds, it will terminate
    supervise(children, strategy: :one_for_all, max_restarts: 1, max_seconds: 200)
  end
end
