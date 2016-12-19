defmodule JsonConsoleBackend do
  @moduledoc """
  A logging backend that outputs JSON to console.

  You can configure it as follows:

    config :logger, :json_console,
      level: :info,
      attributes: [
        app: "db2kafka",
        time: "$date $time",
        level: "$level",
        message: "$message"
      ]

  With the exception of `:level` and `:format`, all the original :console configuration
  still applies, for example:

    config :logger, :console,
      utc_log: true

  """

  use GenEvent

  def init({JsonConsoleBackend, options}) do
    {:ok, configure(options)}
  end

  def init(_) do
    {:ok, configure([])}
  end

  def handle_event({_level, gl, _log_event}, state)
  when node(gl) != node() do
    {:ok, state}
  end

  def handle_event({level, _gl, _log_event} = event, %{out: out} = state) do
    if meet_level?(level, state.level) do
      forward(out, encode_event(event))
    end
    {:ok, state}
  end

  def handle_event(event, %{out: out} = state) do
    forward(out, event)
    {:ok, state}
  end

  ## Helpers

  defp encode_event({level, gl, {logger, message, timeStamp, metadata}}) do
    {level, gl, {logger, encode(message), timeStamp, metadata}}
  end

  defp forward(out, event) do
    GenEvent.notify(out, event)
  end

  defp encode(message) when is_binary message do
    escape_json(message)
  end

  defp encode(message) do
    escape_json(inspect(message))
  end

  defp escape_json(message) do
    quoted = Poison.encode!(message)
    quoted_len = String.length(quoted)
    String.slice(quoted, 1, quoted_len - 2)
  end

  defp meet_level?(_lvl, nil), do: true

  defp meet_level?(lvl, min) do
    Logger.compare_levels(lvl, min) != :lt
  end

  defp configure(options) do
    config = case options do
      [_ | _] -> options
      _ -> Application.get_env(:logger, :json_console, [])
    end
    level = Keyword.get(config, :level)
    format = format_from_attributes(Keyword.get(config, :attributes))
    out = start_console_backend(format, config)
    %{level: level, format: format, out: out}
  end

  defp start_console_backend(format, config) do
    {:ok, out} = GenEvent.start_link([])
    console_device = Keyword.get(config, :device, :user)
    console_options = Keyword.merge(config, [format: format])
    console_args = {console_device, console_options}
    {:ok, _} = Logger.Watcher.watcher(out, Logger.Backends.Console, console_args)
    out
  end

  defp format_from_attributes(raw_attributes) do
    quoted_attributes = for {key, value} <- raw_attributes do
      "#{Poison.encode! key}: #{Poison.encode! value}"
    end
    joined_attributes = Enum.join(quoted_attributes, ", ")
    "{#{joined_attributes}}\n"
  end
end
