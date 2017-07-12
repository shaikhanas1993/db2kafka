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
  barrier_path: "/db2kafka_failover_barrier",
  primary_region: System.get_env("PRIMARY_REGION") || "us-west-2",       # The preferred region
  region: System.get_env("REGION") || "us-west-2",                       # Region this app is running in
  publish_latency_sla_95perc_threshold_ms: 10000,
  publish_latency_sla_max_threshold_ms: 30000

config :kaffe,
  producer: [
    endpoints: [
      {((System.get_env("BROKER_HOST") || "localhost") |> String.to_atom),
        (System.get_env("BROKER_PORT") || "9092") |> String.to_integer
      }
    ], # [hostname: port]
    required_acks: -1,
    topics: (System.get_env("TOPICS") || "foo") |> String.split(","),
  ]
