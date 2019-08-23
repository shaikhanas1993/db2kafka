defmodule Db2Kafka.AppWatcher do
  use GenServer
  require Logger

  @app_name :app_watcher
  @kafka_ex :kafka_ex
  @default_wait_before_restart 1000
  @default_max_restarts 60*60*24 # 24h
  @wait_for_shutdown 100
  @default_health_check_interval 1000

  @initial_state %{
    timer: nil,
    restarts: 0,
  }

  def start_link(args \\ %{}) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(state) do
    timer = start_timer()
    init_state = state
      |> Map.merge(@initial_state)
      |> Map.merge(%{timer: timer})
    {:ok, init_state}
  end

  def before_stop() do 
    GenServer.cast(__MODULE__, :before_stop) 
  end

  def after_stop() do 
    GenServer.cast(__MODULE__, :after_stop) 
  end

  #########

  def handle_info(:tick, %{timer: timer, restarts: restarts} = state) do
    stop_timer(timer)
    restarts = check_app(restarts)
    timer = start_timer()
    new_state = Map.merge(state, %{
      timer: timer, 
      restarts: restarts
    })
    {:noreply, new_state}
  end

  def handle_info(_other, state) do
    {:ok, state}
  end

  def handle_cast(:before_stop, state) do
    on_app_stop()

    {:noreply, state}
  end

  def handle_cast(:after_stop, %{restarts: restarts} = state) do
    Process.sleep(@wait_for_shutdown)

    restarts = check_app(restarts)
    new_state = Map.merge(state, %{restarts: restarts})

    {:noreply, new_state}
  end

  #########

  defp start_timer() do
    Process.send_after(self(), :tick, health_check_interval())
  end

  defp stop_timer(timer) do
    Process.cancel_timer(timer)
  end

  defp on_app_stop() do
    Logger.info("[AW] App stopped")
  end

  defp on_app_restart(_restarted) do
  end

  defp check_app(restarts) do
    restarted = restart_app()
    if !is_nil(restarted), do: on_app_restart(restarted)
    restarts = if restarted == true, do: restarts + 1, else: restarts
    restarts
  end

  defp is_app_started(app) do
    Application.started_applications() |> Enum.find(fn {a, _d, _v} -> a == app end) != nil
  end

  defp restart_app() do
    _is_started = is_app_started(app_name())
    case Application.ensure_started(app_name()) do
      :ok ->
        # still need to check :kafka_ex
        restart_kafkaex()
      {:error, {:not_running, @kafka_ex}} ->
        # first restart depending kafka_ex, then app itself
        do_restart_app(@kafka_ex) and do_restart_app(app_name())
      {:error, _error} ->
        do_restart_app(app_name())
    end
  end

  defp restart_kafkaex() do
    is_started = is_app_started(@kafka_ex)
    if !is_started do
      case Application.ensure_started(@kafka_ex) do
        :ok ->
          Logger.info("[AW] Restarted @kafka_ex")
          nil
        {:error, _error} ->
          do_restart_app(@kafka_ex)
      end
    end
  end

  defp do_restart_app(app, retry_no \\ 0, mode \\ :temporary) do
    is_last_chance = retry_no >= max_restarts() - 1
    is_suicide_mission = mode == :permanent
    Process.sleep(wait_before_restart())
    Logger.info("[AW] Restarting #{inspect app}... #{retry_no + 1}/#{max_restarts()}")
    if is_suicide_mission, do:
      Logger.warn("[AW] Next #{inspect app} shutdown fill be fatal and lead to VM crash")
    case Application.ensure_all_started(app, mode) do
      {:ok, restarted_apps} ->
        Logger.info("[AW] App #{inspect app} restarted: #{inspect restarted_apps}")
        true
      {:error, {^app, {:not_running, @kafka_ex}}} ->
        Logger.error("[AW] App #{inspect app} not restarted because KafkaEx is down")
        do_restart_app(@kafka_ex) and do_restart_app(app, retry_no + 1)
      {:error, error} ->
        Logger.error("[AW] App #{inspect app} not restarted: #{inspect error}")
        cond do
          is_suicide_mission ->
            # VM will crash now
            false
          is_last_chance ->
            # start suicide mission
            do_restart_app(app, retry_no + 1, :permanent)
          true ->
            do_restart_app(app, retry_no + 1)
        end
    end
  end

  defp app_name(), do:
    Application.get_env(@app_name, :protected_app_name)
  defp wait_before_restart(), do:
    Application.get_env(@app_name, :watcher_wait_before_restart, @default_wait_before_restart)
  defp max_restarts(), do:
    Application.get_env(@app_name, :watcher_max_restarts, @default_max_restarts)
  defp health_check_interval(), do:
    Application.get_env(@app_name, :health_check_interval, @default_health_check_interval)
end
