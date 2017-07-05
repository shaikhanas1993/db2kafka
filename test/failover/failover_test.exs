defmodule Failover.Test do
  use ExUnit.Case
  require Logger
  import Mock
  import FailoverMocks
  
  @tag :unit
  test "it initializes failover state correctly" do
    {:ok, state} = with_mock :erlzk_conn, [start_link: fn(_, _, _) -> {:ok, :c.pid(0, 240, 112)} end] do
      Failover.init_state(self(), "/test_db2kafka_barrier")
    end

    assert state[:zk_helper] != nil
    assert state[:instance_pid] == self()
    assert state[:instance_is_probably_doing_work] == false
    assert state[:barrier_path] == "/test_db2kafka_barrier"
  end

  @tag :unit
  test "start_app starts the app" do
    state = make_state()

    assert GenServer.call(state[:instance_pid], :is_working) == false

    {:ok, state} = Failover.start_app(state)

    assert state[:instance_is_probably_doing_work] == true
    assert GenServer.call(state[:instance_pid], :is_working) == true
  end

  @tag :unit
  test "stop_app stops the app" do
    state = make_state()

    assert GenServer.call(state[:instance_pid], :is_working) == false

    {:ok, state} = Failover.start_app(state)

    assert GenServer.call(state[:instance_pid], :is_working) == true

    {:ok, state} = Failover.stop_app(state)
    
    assert GenServer.call(state[:instance_pid], :is_working) == false
  end
end
