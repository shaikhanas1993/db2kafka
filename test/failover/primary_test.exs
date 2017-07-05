defmodule Failover.Primary.Test do
  use ExUnit.Case
  require Logger
  import Mock
  import FailoverMocks

  alias Failover.Primary

  @tag :unit
  test "primary region starts doing work once it's safe and dies when it becomes unsafe" do
    state = make_state()
    
    assert GenServer.call(state[:instance_pid], :is_working) == false
    
    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_create(:success)] do
      Primary.handle_cast(:failover_safe_to_start, state)
    end

    assert GenServer.call(state[:instance_pid], :is_working) == true
    
    {:stop, :zk_disconnected, state} = Primary.handle_cast(:failover_unsafe_to_continue, state)
    
    assert GenServer.call(state[:instance_pid], :is_working) == false
  end
end
