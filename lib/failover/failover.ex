# TODO: add specs
# TODO: see what gets logged when we try to spinnup multiple instances in the same region

defmodule Failover do
  @moduledoc """
  This module provides a GenServer that can be used to handle failover across
  regions or dc's.

  This module provides two callbacks to be handled by the calling service:
  - handle_call(:failover_start, _from, state)
  - handle_call(:failover_stop, _from, state)

  :failover_start
    This event will be sent when your service is required to start doing work.
    That is, your instance will receive this event if:
    - We are configured as the primary instance, in which case it will receive
      this event ASAP.
    - We are configured as a failover and we've noticed the primary is dead.

  :failover_stop
    This event will fire if this instance is doing work and one of these happen:
    - Our zk session has expired
    - We've lost our zk connection.
    - You are a failover instance and we've detected the primary instance is up

  In any of these cases, your service should stop doing work, else you risk
  having both primary and failover instances doing work at the same time.
  """

  use GenServer
  require Logger

  def start_link(instance_pid) do
    Logger.info("Initializing failover")
    GenServer.start_link(__MODULE__, {instance_pid})
  end

  def init({instance_pid}) do
    options =
      [
        monitor: self(),
        # We crash on zk session expire and disconnects and rely
        # on restart to reinitialize us so we don't need these features
        disable_watch_auto_reset: true,
        disable_expire_reconnect: true,
      ]

    zk_host = Application.get_env(:db2kafka, :zk_host) |> to_charlist
    zk_port = Application.get_env(:db2kafka, :zk_port) |> String.to_integer
    zk_hosts = [{zk_host, zk_port}]
    zk_session_timeout = 10000
    {:ok, zk} = :erlzk_conn.start_link(zk_hosts, zk_session_timeout, options)

    initial_state = %{
      zk: zk,
      instance_pid: instance_pid,
      instance_is_probably_doing_work: false,
      primary_region: Application.get_env(:db2kafka, :primary_region),
      region: Application.get_env(:db2kafka, :region),
    }
    {:ok, initial_state}
  end

  def handle_info({:connected, host, port}, state) do
    Logger.info("Connected to ZK, host: #{inspect host}:#{inspect port}")
    post_zk_connect_init(state)
  end

  def handle_info({:disconnected, host, port}, state) do
    Logger.info("Disconnected from ZK, host: #{inspect host}:#{inspect port}")
    {:ok, state} = stop_app(state)
    {:stop, :zk_disconnected, state}
  end

  def handle_info({:expired, host, port}, state) do
    Logger.info("ZK session expired, host: #{inspect host}:#{inspect port}")
    {:ok, state} = stop_app(state)
    {:stop, :zk_session_expired, state}
  end

  def handle_info({:node_created, path}, state) do
    Logger.info("Barrier came up: #{inspect path}, stopping failover region")
    {:ok, state} = stop_app(state)
    prepare_failover(state)
  end

  def handle_info({:node_deleted, path}, state) do
    Logger.info("Barrier came down: #{inspect path}, starting failover region")
    prepare_failover(state)
  end

  defp post_zk_connect_init(state) do
    %{primary_region: primary_region, region: region} = state

    if primary_region == region do
      Logger.info("Attempting to start primary region")
      prepare_primary(state)
    else
      Logger.info("Attempting to start failover region")
      prepare_failover(state)
    end
  end

  defp prepare_primary(%{zk: zk}=state) do
    Logger.info("Attempting to create barrier #{inspect barrier_path(state)}")
    case create_barrier(zk, barrier_path(state)) do
      {:ok, path} ->
        Logger.info("Created barrier #{path}, starting primary region")
        {:ok, state} = start_app(state)
        {:noreply, state}
      {:error, reason} ->
        # For all the error values see:
        # https://github.com/huaban/erlzk/blob/master/src/erlzk.erl#L154
        #
        # The error we will most likely receive is :node_exists.
        # This can happen if the instance dies and restarts before the zk
        # session expires. In that case the barrier node will still exist
        # as the nodes life is tied to the session life.
        #
        # This can also happen if the primary instance is already running
        #
        # To handle this we crash and rely on the cluster manager to restart
        # us, hopefully the situation is better on next startup
        Logger.info("Failed to create barrier, reason: #{inspect reason}")
        {:stop, :failed_to_create_barrier, state}
    end
  end

  defp prepare_failover(%{zk: zk}=state) do
    Logger.info("Checking state of barrier #{inspect barrier_path(state)}")
    case watch_barrier(zk, barrier_path(state)) do
      {:ok, _stat} ->
        Logger.info("Barrier exists, waiting for barrier to go down")
        {:noreply, state}
      {:error, :no_node} ->
        Logger.info("No barrier, this implies primary is down")
        {:ok, state} = start_app(state)
        {:noreply, state}
    end
  end

  defp barrier_path(%{primary_region: primary_region}) do
    "/failover_#{primary_region}"
  end

  defp start_app(%{instance_pid: pid}=state) do
    Logger.info("Starting app (in #{inspect state[:region]})")
    :ok = GenServer.call(pid, :failover_start)
    {:ok, %{state | instance_is_probably_doing_work: true}}
  end

  defp stop_app(%{instance_pid: pid}=state) do
    if state[:instance_is_probably_doing_work] do
      Logger.info("Stopping app (in #{inspect state[:region]})")
      :ok = GenServer.call(pid, :failover_stop)
    end
    {:ok, %{state | instance_is_probably_doing_work: false}}
  end

  defp create_barrier(zk, path) do
    acl = {:rwcda, 'world', 'anyone'}
    
    case :erlzk_conn.create(zk, to_charlist(path), "", [acl], :ephemeral) do
      {:ok, path} -> {:ok, to_string(path)}
      error -> error
    end
  end

  defp watch_barrier(zk, path) do
    :erlzk_conn.exists(zk, to_charlist(path), true, self())
  end
end
