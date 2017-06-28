use Mix.Config

#
# For now, we rely on environment vars to do
# configuration.
#

config :db2kafka,
  db_hostname:  System.get_env("DB_HOST") || "localhost",                # MySQL hostname
  db_username:  System.get_env("DB_USER") || "root",                     # MySQL username
  db_password:  System.get_env("DB_PASS") || "",                         # MySQL password
  db_name:      System.get_env("DB_NAME") || "db2kafka",                 # MySQL database
  topics:      (System.get_env("TOPICS") || "foo") |> String.split(","), # Comma-separated list of topics

  # Failover configuration
  zk_hosts: [                                                            # List known ZK hosts
    { System.get_env("ZK_HOST") || "localhost",
     (System.get_env("ZK_PORT") || "2181") |> String.to_integer
    }
  ],
  primary_region: System.get_env("PRIMARY_REGION") || "us-west-2",       # The preferred region
  region: System.get_env("REGION") || "us-west-2"                        # Region this app is running in

config :kafka_ex,
  brokers: [
    { System.get_env("BROKER_HOST") || "localhost",                      # A Kafka bootstrap broker hostname
     (System.get_env("BROKER_PORT") || "9092") |> String.to_integer}     # A Kafka bootstrap broker port
  ],
  # Tweak kafka_ex for our needs - don't override unless you know exactly what's going on!
  consumer_group: :no_consumer_group,
  disable_default_worker: false

