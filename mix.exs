defmodule Db2Kafka.Mixfile do
  use Mix.Project


  def project do
    [app: :db2kafka,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps
    ]
  end

  def application do
    [applications: [:logger, :kafka_ex, :ex_statsd, :crypto, :poolboy],
     included_applications: [ :mariaex, :murmur, :poison],
     mod: {Db2Kafka, []}
    ]
  end

  defp deps do
    [
      {:mock, "~> 0.1.1", only: :test},
      {:kafka_ex, "~> 0.5.0"},
      {:murmur, "~> 1.0"},
      {:mariaex, "~> 0.7.3"},
      {:ex_statsd, "~> 0.5.1"},
      {:poolboy, "~> 1.5"},
      {:dialyxir, "~> 0.3.5", only: [:dev]},
      {:poison, "~> 2.0"}
    ]
  end
end
