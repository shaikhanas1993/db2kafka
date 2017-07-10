defmodule Failover.ZKBarrier do
  @moduledoc """
  This module provides a GenServer that attempts to simplify common interactions with the erlzk client in the context of failover.

  In particular, it provides the callbacks to notify when we have the gurantee of an active ZK session:

  handle_cast(:zk_session_ready, state)   # sent once our ZK session is active
  handle_cast(:zk_state_uncertain, state) # sent when our ZK session has expired or we've lost our ZK connection
  """

  use GenServer
  require Logger

  def start_link(monitor) do
    Logger.info("Initializing failover ZKBarrier")
    GenServer.start_link(__MODULE__, monitor)
  end

  def init(monitor) do
    options =
      [
        monitor: self(),
        # We crash on zk session expire and disconnects and rely
        # on restart to reinitialize us so we don't need these features
        disable_watch_auto_reset: true,
        disable_expire_reconnect: true,
      ]

    zk_session_timeout = 10000
    zk_hosts =
       Application.get_env(:db2kafka, :zk_hosts)
       |> Enum.map(fn {host, port} -> { to_charlist(host), port } end)

    {:ok, zk} = :erlzk_conn.start_link(zk_hosts, zk_session_timeout, options)

    initial_state = %{
      zk: zk,
      monitor: monitor
    }
    {:ok, initial_state}
  end

  def handle_info({:connected, host, port}, state) do
    Logger.info("Connected to ZK, host: #{inspect host}:#{inspect port}")
    GenServer.cast(state[:monitor], :zk_session_ready)
    {:noreply, state}
  end

  def handle_info({:disconnected, host, port}, state) do
    Logger.info("Disconnected from ZK, host: #{inspect host}:#{inspect port}")
    GenServer.cast(state[:monitor], :zk_state_uncertain)
    {:stop, :zk_disconnected, state}
  end

  def handle_info({:expired, host, port}, state) do
    Logger.info("ZK session expired, host: #{inspect host}:#{inspect port}")
    GenServer.cast(state[:monitor], :zk_state_uncertain)
    {:stop, :zk_session_expired, state}
  end

  def handle_call({:create_barrier, path}, _from, state) do
    Logger.info("creating barrier: #{inspect path}")
    res = create_barrier(state[:zk], path)
    {:reply, res, state}
  end

  @doc """
  watcher will must handle callbacks:
    handle_info(:barrier_came_up, state)   # sent when the barrier node comes up
    handle_info(:barrier_came_down, state) # sent when the barrier node goes down

  At most one of these callbacks will be sent, if the barrier node does not exist at
  the time of the watch being placed, :barrier_came_up will be sent if the node appears.

  If the node does exist at time of watch being place, a :barrier_came_down message will
  be sent when the barrier node comes disappers

  After a watch triggers, you must set another watch to be notified of future changes
  to the barrier node.

  replies with one of:
    :barrier_is_up   # barrier was up at time of watch being set
    :barrier_is_down # barrier was down at time of watch being set
  """
  def handle_call({:watch_barrier, path, watcher}, _from, state) do
    {:reply, watch_barrier(state[:zk], path, watcher), state}
  end

  defp create_barrier(zk, path) do
    acl = {:rwcda, 'world', 'anyone'}

    res = :erlzk_conn.create(zk, to_charlist(path), "", [acl], :ephemeral)

    case res do
      {:ok, path} -> {:ok, to_string(path)}
      error -> error
    end
  end

  defp watch_barrier(zk, path, watcher) do
    erlzk_message_translator = fn ->
      receive do
	{:node_created, _path} -> send(watcher, :barrier_came_up)
	{:node_deleted, _path} -> send(watcher, :barrier_came_down)
      end
    end

    res = :erlzk_conn.exists(zk, to_charlist(path), true, spawn erlzk_message_translator)
    case res do
      {:ok, _stat} -> :barrier_is_up
      {:error, :no_node} -> :barrier_is_down
    end
  end
end
