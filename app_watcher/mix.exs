defmodule Db2Kafka.AppWatcher.Mixfile do
  use Mix.Project

  def project do
    [app: :app_watcher,
     version: "0.1.0",
     elixir: "~> 1.7.4",
     start_permanent: true,
     deps: deps(),
     description: "Auto restart main app",
     build_path: "../_build",
     deps_path: "../deps",
     lockfile: "../mix.lock",
    ]
  end

  def application do
    [
      applications: [:logger],
      included_applications: [],
      mod: {Db2Kafka.AppWatcher.Application, []}
    ]
  end

  defp deps do
    [
    ]
  end

end
