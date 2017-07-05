defmodule Failover.Secondary do
  use GenServer
  require Logger

  def start_link(instance_pid, barrier_path) do
    Logger.info("Initializing Secondary")
    GenServer.start_link(__MODULE__, {instance_pid, barrier_path})
  end

  def init({instance_pid, barrier_path}) do
    Failover.init_state(instance_pid, barrier_path)
  end

  def handle_cast(:failover_safe_to_start, state) do
    Logger.info("Received go-ahead from zk helper")
    {:ok, state} = prepare_failover(state)
    {:noreply, state}
  end

  def handle_cast(:failover_unsafe_to_continue, state) do
    Logger.info("Unsafe to continue working, shutting down")
    {:ok, state} = Failover.stop_app(state)
    {:stop, :zk_disconnected, state}
  end

  def handle_info({:node_created, path}, state) do
    Logger.info("Barrier came up: #{inspect path}, stopping failover region")
    {:ok, state} = Failover.stop_app(state)
    {:ok, state} = prepare_failover(state)
    {:noreply, state}
  end

  def handle_info({:node_deleted, path}, state) do
    Logger.info("Barrier came down: #{inspect path}, starting failover region")
    {:ok, state} = prepare_failover(state)
    {:noreply, state}
  end

  defp prepare_failover(%{zk_helper: zk_helper, barrier_path: barrier_path}=state) do
    Logger.info("Checking state of barrier #{inspect barrier_path}")
    res = GenServer.call(zk_helper, {:watch_barrier, barrier_path, self()})
    case res do
      {:ok, _stat} ->
        Logger.info("Barrier exists, waiting for barrier to go down")
        {:ok, state}
      {:error, :no_node} ->
        Logger.info("No barrier, this implies primary is down")
        {:ok, state} = Failover.start_app(state)
        {:ok, state}
    end
  end
end
