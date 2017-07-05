defmodule Failover.Primary do
  use GenServer
  require Logger

  def start_link(instance_pid, barrier_path) do
    Logger.info("Initializing Primary")
    GenServer.start_link(__MODULE__, {instance_pid, barrier_path})
  end

  def init({instance_pid, barrier_path}) do
    Failover.init_state(instance_pid, barrier_path)
  end

  def handle_cast(:failover_safe_to_start, state) do
    Logger.info("Received go-ahead from zk helper, preparing to start working")
    {:ok, state} = prepare_failover(state)
    {:noreply, state}
  end

  def handle_cast(:failover_unsafe_to_continue, state) do
    Logger.info("Unsafe to continue working, shutting down")
    {:ok, state} = Failover.stop_app(state)
    {:stop, :zk_disconnected, state}
  end

  defp prepare_failover(%{zk_helper: zk_helper, barrier_path: barrier_path}=state) do
    Logger.info("Attempting to create barrier #{inspect barrier_path}")

    res = GenServer.call(zk_helper, {:create_barrier, barrier_path})
    
    case res do
      {:ok, path} ->
        Logger.info("Created barrier #{path}, starting primary region")
        {:ok, state} = Failover.start_app(state)
        {:ok, state}
      {:error, reason} ->
        # For all the error reasons see:
        # https://github.com/huaban/erlzk/blob/master/src/erlzk.erl#L154
        #
        # The error we will most likely receive is :node_exists.
        # This can happen if the instance dies and restarts before the zk
        # session expires. In that case the barrier node will still exist
        # as the nodes life is tied to the session life.
        #
        # This can also happen if the primary instance is already running
        #
        # To handle this we crash and rely on the process manager to restart
        # us, hopefully the situation is better on next startup
        Logger.info("Failed to create barrier, reason: #{inspect reason}")
        {:error, :failed_to_create_barrier, state}
    end
  end
end
