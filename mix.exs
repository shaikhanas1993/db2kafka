defmodule Db2Kafka.Mixfile do
  use Mix.Project


  def project do
    [app: :db2kafka,
     version: "0.5.0",
     elixir: "~> 1.7.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),
     dialyzer: [plt_file: ".plts/local.plt"],
     description: "A MySQL to Kafka data pump",
     package: package(),
     aliases: aliases()
    ]
  end

  def application do
    [applications: [:logger, :kafka_ex, :ex_statsd_pd, :crypto, :poolboy],
     included_applications: [:mariaex, :murmur, :poison, :pd_erlzk],
     mod: {Db2Kafka, []}
    ]
  end

  defp deps do
    [
      {:mock, "~> 0.1.1", only: :test},
      {:kafka_ex, "~> 0.5.0"},
      {:murmur, "~> 1.0"},
      {:mariaex, "~> 0.7.3"},
      {:ex_statsd_pd, "~> 0.6.0"}, # using PD version of the lib as the maintainers hasn't published a new release yet
      {:poolboy, "~> 1.5"},
      {:dialyxir, "~> 0.3.5", only: [:dev]},
      {:poison, "~> 2.0"},
      {:ex_doc, ">= 0.0.0", only: :dev},
      {:pd_erlzk, "~> 0.6.4"} # using PD version of the lib as the maintainers hasn't published a new release yet
    ]
  end

  defp package do
    [maintainers: ["PagerDuty"],
     licenses: ["MIT"],
     links: %{
      "GitHub": "https://github.com/PagerDuty/db2kafka",
      "README": "https://github.com/PagerDuty/db2kafka/blob/master/README.md"}]
  end

  defp aliases do
    [publish: [&git_tag/1, "hex.publish"], version: &version/1]
  end

  defp git_tag(_args) do
    version = Mix.Project.config[:version]
    tag = "v#{version}"
    {_, 0} = System.cmd "git", ["tag", tag, "-m", "Release version #{version}"]
    {_, 0} = System.cmd "git", ["push", "origin", tag]
  end

  defp version(_args) do
    IO.puts project()[:version]
  end
end
