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
    Db2Kafka.start
  end
end

defmodule RecordCreator do
  use GenServer
  require Logger

  @table "outbound_kafka_queue"

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

Application.start(:ex_statsd)
ExUnit.start([capture_log: true])
