defmodule Failover.ZKHelper do
  @moduledoc """
  This module provides a GenServer that attempts to simplify common interactions with the erlzk client in the context of failover.

  In particular, it provides the callbacks to notify when we have the gurantee of an active ZK session:

  handle_cast(:failover_safe_to_start, state)      # sent once our ZK session is active
  handle_cast(:failover_unsafe_to_continue, state) # sent when our ZK session has expired or our ZK connection has died
  """
  use GenServer
  require Logger

  def start_link(failover) do
    Logger.info("Initializing failover ZKHelper")
    GenServer.start_link(__MODULE__, failover)
  end

  def init(failover) do
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
      failover: failover
    }
    {:ok, initial_state}
  end

  def handle_info({:connected, host, port}, state) do
    Logger.info("Connected to ZK, host: #{inspect host}:#{inspect port}")
    GenServer.cast(state[:failover], :failover_safe_to_start)
    {:noreply, state}
  end

  def handle_info({:disconnected, host, port}, state) do
    Logger.info("Disconnected from ZK, host: #{inspect host}:#{inspect port}")
    GenServer.cast(state[:failover], :failover_unsafe_to_continue)
    {:stop, :zk_disconnected, state}
  end

  def handle_info({:expired, host, port}, state) do
    Logger.info("ZK session expired, host: #{inspect host}:#{inspect port}")
    GenServer.cast(state[:failover], :failover_unsafe_to_continue)
    {:stop, :zk_session_expired, state}
  end

  def handle_call({:create_barrier, path}, _from, state) do
    Logger.info("creating barrier: #{inspect path}")
    res = create_barrier(state[:zk], path)
    {:reply, res, state}
  end

  def handle_call({:watch_barrier, path, watcher}, _from, state) do
    {:reply, watch_barrier(state[:zk], path, watcher), state}
  end
  
  defp create_barrier(zk, path) do
    acl = {:rwcda, 'world', 'anyone'}
    
    res =
      case :erlzk_conn.create(zk, to_charlist(path), "", [acl], :ephemeral) do
	{:ok, path} -> {:ok, to_string(path)}
	error -> error
      end
    
    res
  end

  defp watch_barrier(zk, path, watcher) do
    :erlzk_conn.exists(zk, to_charlist(path), true, watcher)
  end
end
