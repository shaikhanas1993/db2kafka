defmodule JsonConsoleBackend.Test do
  use ExUnit.Case, async: false

  require Logger
  import Eventually

  defp attributes do
    {:attributes, [
      env: "env\"",
      app: "app\"",
      time: "time\"",
      level: "level\"",
      message: "$message\""
    ]}
  end

  setup_all _context do
    device = capture_logging_output(attributes)
    on_exit fn -> restore_logging_output(device) end
    {:ok, device: device}
  end

  @tag :logging
  test "format is loaded correctly" do
    option_list = [attributes]
    {:ok, %{format: format}} = JsonConsoleBackend.init({JsonConsoleBackend, option_list})
    expected_attributes = ~S("env": "env\"", "app": "app\"", "time": "time\"", "level": "level\"", "message": "$message\"")
    expected_format = "{#{expected_attributes}}\n"
    assert format == expected_format
  end

  @tag :logging
  test "forward escaped message correctly", %{device: device} do
    unescaped_message = "forward escaped message correctly\""
    escaped_message = "forward escaped message correctly\\\""
    Logger.info(unescaped_message)

    expected_log_entry = ~S({"env": "env\"", "app": "app\"", "time": "time\"", "level": "level\"", "message": ) <> "\"#{escaped_message}\\\"\"}"
    assert_eventually_has_been_logged(device, expected_log_entry)
  end

  @tag :logging
  test "support logging non-strings", %{device: device} do
    Logger.error(["support", ["logging", "non-strings"]])
    expected_log_entry = ~S({"env": "env\"", "app": "app\"", "time": "time\"", "level": "level\"", "message": "[\"support\", [\"logging\", \"non-strings\"]]\""})

    assert_eventually_has_been_logged(device, expected_log_entry)
  end

  defp assert_eventually_has_been_logged(device, expected_log_entry) do
    assert_eventually(1_000, fn ->
      StubDevice.get_captured_logs(device)
        |> Enum.find(fn(log_entry) -> String.contains?(log_entry, expected_log_entry) end)
    end)
  end

  defp capture_logging_output(attributes) do
    {:ok, device} = StubDevice.start
    Logger.remove_backend(JsonConsoleBackend)
    option_list = [{:device, device}, attributes]
    Logger.add_backend({JsonConsoleBackend, option_list})
    device
  end

  defp restore_logging_output(device) do
    StubDevice.stop(device)
    reload_logging_backend()
  end

  defp reload_logging_backend() do
    Logger.remove_backend(JsonConsoleBackend)
    Logger.add_backend(JsonConsoleBackend)
  end
end

defmodule StubDevice do
  def start do
    Task.start(fn -> loop([]) end)
  end

  def get_captured_logs(device) do
    send device, {:get_captured, self}
    receive do
      {:captured, captured} -> captured
    end
  end

  def stop(device) do
    send device, :stop
  end

  defp loop(captured) do
    receive do
      {:io_request, sender, ref, {:put_chars, :unicode, char_data}} ->
        send(sender, {:io_reply, ref, :ok})
        loop [to_string(char_data) | captured]
      {:get_captured, caller} ->
        send caller, {:captured, captured}
        loop(captured)
      :stop ->
        :ok
      msg ->
        IO.puts :stderr, "Unexpected message: #{inspect(msg)}."
        loop captured
    end
  end
end
