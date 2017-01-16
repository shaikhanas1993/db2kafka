defmodule Db2Kafka.Mixfile do
  use Mix.Project


  def project do
    [app: :db2kafka,
     version: "0.2.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps,
     dialyzer: [plt_file: ".plts/local.plt"],
     description: "A MySQL to Kafka data pump",
     package: package()
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
      {:poison, "~> 2.0"},
      {:ex_doc, ">= 0.0.0", only: :dev}
    ]
  end

  defp package do
    [maintainers: ["PagerDuty"],
     licenses: ["MIT"],
     links: %{
      "GitHub": "https://github.com/PagerDuty/db2kafka",
      "README": "https://github.com/PagerDuty/db2kafka/blob/master/README.md"}]
  end
end
