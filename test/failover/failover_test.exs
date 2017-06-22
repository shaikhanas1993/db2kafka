defmodule Failover.Test do
  use ExUnit.Case
  require Logger
  import Mock

  def make_failover_state() do
    {:ok, failover_helper} = FailoverHelper.start_link()
    %{
      zk: :c.pid(0,240,113), # dummmy zk pid
      instance_pid: failover_helper,
      instance_is_probably_doing_work: false,
      primary_region: "us-west-2",
      region: "us-west-1"
    }
  end
  
  def make_primary_state() do
    state = make_failover_state()
    %{ state | region: state[:primary_region] }
  end

  def mock_zk_create(:success) do
    {:create, fn (_zk, path, _data, _acls, _type) -> {:ok, path} end}
  end
  
  def mock_zk_create(:fail) do
    {:create, fn (_zk, path, _data, _acls, _type) -> {:error, :node_exists} end}
  end
  
  def mock_zk_exists(:exists) do
    {:exists, fn (_zk, _path, _watch, _watcher) -> {:ok, :dummy_node_stat} end}
  end

  def mock_zk_exists(:not_exists) do
    {:exists, fn (_zk, _path, _watch, _watcher) -> {:error, :no_node} end}
  end
  
  @tag :unit
  test "it initializes failover state correctly" do
    expected_state = make_failover_state()
    
    Application.put_env(:db2kafka, :primary_region, expected_state[:primary_region])
    Application.put_env(:db2kafka, :region, expected_state[:region])
    
    with_mock :erlzk_conn, [start_link: fn(_, _, _) -> {:ok, expected_state[:zk]} end] do
      assert Failover.init(expected_state[:instance_pid]) == {:ok, expected_state}
    end
  end

  @tag :unit
  test "primary region starts doing work once we get a zk connection and dies when we become disconnected" do
    state = make_primary_state()
    
    assert GenServer.call(state[:instance_pid], :is_working) == false
    
    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_create(:success)] do
      Failover.handle_info({:connected, 'localhost', 2181}, state)
    end

    assert GenServer.call(state[:instance_pid], :is_working) == true
    
    {:stop, :zk_disconnected, state} = Failover.handle_info({:disconnected, 'localhost', 2181}, state)
    
    assert GenServer.call(state[:instance_pid], :is_working) == false
  end

  @tag :unit
  test "if primary region is down, failover region starts doing work on zk connection" do
    state = make_failover_state()
    
    assert GenServer.call(state[:instance_pid], :is_working) == false

    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:not_exists)] do
      Failover.handle_info({:connected, 'localhost', 2181}, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == true
  end
  
  @tag :unit
  test "if primary region is up, failover region doesn't start doing work" do
    state = make_failover_state()
    
    assert GenServer.call(state[:instance_pid], :is_working) == false

    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:exists)] do
      Failover.handle_info({:connected, 'localhost', 2181}, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == false
  end

  @tag :unit
  test "if primary goes down, failover should start doing work" do
    state = make_failover_state()

    assert GenServer.call(state[:instance_pid], :is_working) == false
    
    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:not_exists)] do
      Failover.handle_info({:node_deleted, '/failover'}, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == true
  end

  @tag :unit
  test "if failover is up and primary comes up, failover should stop working" do
    state = make_failover_state()

    
    assert GenServer.call(state[:instance_pid], :is_working) == false

    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:not_exists)] do
      Failover.handle_info({:connected, 'localhost', 2181}, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == true

    {:noreply, state} = with_mock :erlzk_conn, [mock_zk_exists(:exists)] do
      Failover.handle_info({:node_created, '/failover'}, state)
    end
    
    assert GenServer.call(state[:instance_pid], :is_working) == false
  end
end
