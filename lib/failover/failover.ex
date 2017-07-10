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
  require Logger
  
  def start_link(instance_pid) do
    barrier_path = Application.get_env(:db2kafka, :barrier_path)
    primary_region = Application.get_env(:db2kafka, :primary_region)
    region = Application.get_env(:db2kafka, :region)
    
    if primary_region == region do
      Logger.info("Starting as primary (in #{region})")
      Failover.Primary.start_link(instance_pid, barrier_path)
    else
      Logger.info("Starting as secondary (in #{region})")
      Failover.Secondary.start_link(instance_pid, barrier_path)
    end
  end

  # Common functionality across all failover run modes
  
  def init_state(instance_pid, barrier_path) do
    {:ok, zk_barrier} = Failover.ZKBarrier.start_link(self())
    
    initial_state = %{
      zk_barrier: zk_barrier,
      instance_pid: instance_pid,
      instance_is_probably_doing_work: false,
      barrier_path: barrier_path,
    }
    {:ok, initial_state}
  end
  
  def start_app(state) do
    Logger.info("Starting app")
    :ok = GenServer.call(state[:instance_pid], :failover_start)
    {:ok, %{state | instance_is_probably_doing_work: true}}
  end

  def stop_app(state) do
    if state[:instance_is_probably_doing_work] do
      Logger.info("Stopping app")
      :ok = GenServer.call(state[:instance_pid], :failover_stop)
    end
    {:ok, %{state | instance_is_probably_doing_work: false}}
  end
end
