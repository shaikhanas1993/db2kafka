defmodule Failover.Secondary.Test do
  use ExUnit.Case
  require Logger
  import Mock
  import FailoverMocks

  alias Failover.Secondary
  @tag :unit
  test "if primary is down, secondary starts working when it's safe" do
    state = make_state()
    
    assert GenServer.call(state[:instance_pid], :is_working) == false

    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:not_exists)] do
      Secondary.handle_cast(:zk_session_ready, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == true
  end
  
  @tag :unit
  test "if primary is up, secondary doesn't start working" do
    state = make_state()
    
    assert GenServer.call(state[:instance_pid], :is_working) == false

    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:exists)] do
      Secondary.handle_cast(:zk_session_ready, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == false
  end

  @tag :unit
  test "if primary goes down, secondary should start working" do
    state = make_state()

    assert GenServer.call(state[:instance_pid], :is_working) == false
    
    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:not_exists)] do
      Secondary.handle_info(:barrier_came_down, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == true
  end

  @tag :unit
  test "if secondary is up and primary comes up, secondary should stop working" do
    state = make_state()

    assert GenServer.call(state[:instance_pid], :is_working) == false

    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:not_exists)] do
      Secondary.handle_cast(:zk_session_ready, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == true

    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:exists)] do
      Secondary.handle_info(:barrier_came_up, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == false
  end
end
