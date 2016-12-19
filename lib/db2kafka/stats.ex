defmodule Db2Kafka.Stats do

  @default_sample_rate 1.0 # A value of 1.0 means no sampling. Sampling is bad, as it leads to misleading DD metrics!
  @gauge_sleep_time_secs 10

  use GenServer

  @spec start_link(String.t) :: GenServer.on_start
  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: __MODULE__)
  end

  @spec incrementSuccess(String.t, float) :: nil
  def incrementSuccess(metric_name, sample_rate \\ @default_sample_rate) do
    increment(metric_name, ["result:success"], sample_rate)
  end

  @spec incrementFailure(String.t, float) :: nil
  def incrementFailure(metric_name, sample_rate \\ @default_sample_rate) do
    increment(metric_name, ["result:failure"], sample_rate)
  end

  @spec incrementCountBy(String.t, number, [String.t], float) :: nil
  def incrementCountBy(metric_name, amount, tags \\ [], sample_rate \\ @default_sample_rate) do
    amount |> ExStatsD.counter(metric_name <> "_count", [sample_rate: sample_rate, tags: tags])
  end

  @spec timing(String.t, (() -> ret)) :: ret when ret: any
  def timing(metric_name, fnToTime) do
    ExStatsD.timing(metric_name <> "_msec", fnToTime)
  end

  @spec timer(String.t, number, float) :: nil
  def timer(metric_name, age, sample_rate \\ @default_sample_rate) do
    ExStatsD.timer(age, metric_name <> "_sec", [sample_rate: sample_rate])
  end

  @spec increment(String.t, [String.t], float) :: nil
  defp increment(metric_name, tags, sample_rate) do
    ExStatsD.increment(metric_name <> "_count", [sample_rate: sample_rate, tags: tags])
  end

  def histogram(amount, metric_name) do
    ExStatsD.histogram(amount, metric_name)
  end

  #  Server side. Run a simple wait loop and emit stats about the Erlang VM every X seconds
  #  https://github.com/Amadiro/erlang-statistics/blob/master/erlang/statistics.erl has
  #  a nice list of stuff you can spam DogStatsD with :)
  def init(name) do
    schedule_work()
    {:ok, name}
  end

  def handle_info(:work, name) do
    send_gauges(name)
    schedule_work()
    {:noreply, name}
  end

  defp schedule_work() do
    Process.send_after(self(), :work, @gauge_sleep_time_secs * 1000)
  end

  defp send_gauges(name) do
    send_erlang_memory_gauges(name)
    send_erlang_process_gauges(name)
  end

  defp send_erlang_memory_gauges(name) do
    :erlang.memory
      |> Enum.each(fn {key, val} -> gauge(name, "memory_" <> Atom.to_string(key), val) end)
  end

  defp send_erlang_process_gauges(name) do
    gauge(name, "run_queue", :erlang.statistics(:run_queue))
    gauge(name, "process_count", :erlang.system_info(:process_count))
    gauge(name, "process_limit", :erlang.system_info(:process_limit))
  end

  defp gauge(name, key, val) do
    ExStatsD.gauge(val, name <> "." <> key, [sample_rate: @default_sample_rate])
  end
end
