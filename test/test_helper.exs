defmodule Eventually do
  import ExUnit.Assertions

  @default_timeout 200
  @check_period 10

  def assert_eventually(fun), do: assert_eventually(@default_timeout, fun)

  def assert_eventually(0, fun), do: assert(fun.())

  def assert_eventually(timeout, fun) do
    try do
      assert(fun.())
    rescue
      ExUnit.AssertionError ->
        :timer.sleep(@check_period)
        assert_eventually(max(0, timeout - @check_period), fun)
    end
  end

  def refute_always(fun), do: refute_always(@default_timeout, fun)

  def refute_always(0, fun) do
    test_refutation(fun)
  end

  def refute_always(timeout, fun) do
    test_refutation(fun)
    :timer.sleep(@check_period)
    refute_always(max(0, timeout - @check_period), fun)
  end

  defp test_refutation(fun) do
    if fun.() == true do
      raise ExUnit.AssertionError.exception("refute_always condition was true when expected to be false")
    end
  end
end

defmodule TestHelpers do
  require Logger
  def create_db_pid do
    db_username = Application.get_env(:db2kafka, :db_username)
    db_password = Application.get_env(:db2kafka, :db_password)
    db_name = Application.get_env(:db2kafka, :db_name)

    {:ok, db_pid} = Mariaex.start_link(
      username: db_username,
      password: db_password,
      database: db_name
    )

    {:ok, db_pid}
  end

  def start_apps do
    Logger.info("Starting applications")
    Db2Kafka.Mixfile.application[:applications]
      |> Enum.map(&Application.start(&1))
      Application.ensure_all_started(:kaffe)
    Db2Kafka.start
  end
end

defmodule RecordCreator do
  use GenServer
  require Logger

  def start_link do
    Logger.info("Starting RecordCreator")
    GenServer.start_link(__MODULE__, [], [name: __MODULE__])
  end

  def init(_state) do
    TestHelpers.create_db_pid
  end

  def create_records(records) do
    GenServer.call(__MODULE__, {:create_records, records})
  end

  def handle_call({:create_records, records}, _from, db_pid) do
    Enum.each(records, fn(r) ->
      {:ok, _} = Mariaex.query(db_pid, "INSERT INTO `outbound_kafka_queue` " <>
        "(`id`, `topic`, `item_key`, `item_body`, `created_at`) VALUES " <>
        "('#{r.id}', '#{r.topic}', '#{r.partition_key}', '#{r.body}', from_unixtime(#{r.created_at}))"
      )
    end)
    {:reply, :ok, db_pid}
  end
end

defmodule DatabaseHelper do
  require Logger

  def drop_and_create_table do
    {:ok, db_pid} = TestHelpers.create_db_pid

    Logger.info("Recreating Schema")
    Mariaex.query(db_pid, "DROP TABLE IF EXISTS `outbound_kafka_queue`")
    Mariaex.query(db_pid, "CREATE TABLE `outbound_kafka_queue` (" <>
    " `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT," <>
    " `topic` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL," <>
    " `item_key` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL," <>
    " `item_body` varbinary(60000) DEFAULT NULL," <>
    " `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," <>
    " PRIMARY KEY (`id`)" <>
    ")")

    Process.exit(db_pid, :normal)

    :ok
  end
end

defmodule FailoverHelper do
  use GenServer

  def start_link do
    init_state = %{
      doing_work: false,
      safe_to_do_work: false,
    }
    GenServer.start_link(__MODULE__, init_state)
  end

  def init(state) do
    {:ok, state}
  end

  def handle_call(:is_working, _from, state) do
    {:reply, state[:doing_work], state}
  end
  
  def handle_call(:safe_to_work, _from, state) do
    {:reply, state[:safe_to_do_work], state}
  end
  
  def handle_call(:failover_start, _from, state) do
    {:reply, :ok, %{state | doing_work: true}}
  end

  def handle_call(:failover_stop, _from, state) do
    {:reply, :ok, %{state | doing_work: false}}
  end

  def handle_cast(:zk_session_ready, state) do
    {:noreply, %{state | safe_to_do_work: true}}
  end

  def handle_cast(:zk_state_uncertain, state) do
    {:noreply, %{state | safe_to_do_work: false}}
  end
end

defmodule FailoverMocks do
  import ExUnit.Assertions
  import Mock
  
  @zk :c.pid(0, 240, 113)

  def make_state() do
    {:ok, failover_helper} = FailoverHelper.start_link()

    {:ok, zk_barrier} = with_mock :erlzk_conn, [start_link: fn(_, _, _) -> {:ok, @zk} end] do
      Failover.ZKBarrier.start_link(failover_helper)
    end
    
    %{
      zk_barrier: zk_barrier,
      instance_pid: failover_helper,
      instance_is_probably_doing_work: false,
      barrier_path: "/db2kafka_failover_barrier"
    }
  end

  def make_zk_barrier_state() do
    {:ok, failover_helper} = FailoverHelper.start_link()

    %{zk: @zk, monitor: failover_helper}
  end

  def mock_zk_create(:success) do
    {:create, fn (_zk, path, _data, _acls, _type) -> {:ok, path} end}
  end
  
  def mock_zk_create(:fail) do
    {:create, fn (_zk, _path, _data, _acls, _type) -> {:error, :node_exists} end}
  end
  
  def mock_zk_exists(:exists) do
    {:exists, fn (_zk, _path, _watch, _watcher) -> {:ok, :dummy_node_stat} end}
  end

  def mock_zk_exists(:not_exists) do
    {:exists, fn (_zk, _path, _watch, _watcher) -> {:error, :no_node} end}
  end
end

Application.start(:ex_statsd_pd)
ExUnit.start([capture_log: true])
