defmodule FlezhaExamplePlugin.Stamp do
  @moduledoc """
  Appends every output payload of the relay it is enabled on to a file, one
  line each, prefixed with the stamp `stamp_lib` renders.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.WorkerSup
  alias FlezhaExamplePlugin.Stamp.Writer

  use Flezha.Plugin.ProviderBehaviour,
    options: [path: [type: :string, required: true, doc: "File the stamped lines go to"]],
    worker: [module: Writer, restart: :transient]

  @impl true
  def init_provider(%{path: ""}), do: {:error, :stamp_path_empty}
  def init_provider(opts), do: {:ok, opts}

  # One writer per plugin, under the id it was enabled with.
  @impl true
  def worker_specs(opts), do: [{opts.source_plugin_id, opts}]

  @impl true
  def on_output(%Ctx.IO{message: payload} = ctx, opts) do
    Writer.append(WorkerSup.worker(__MODULE__, opts.source_plugin_id), payload)
    {:ok, ctx}
  end
end

defmodule FlezhaExamplePlugin.Stamp.Writer do
  @moduledoc "The provider's worker: the one process writing to its file."

  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts.path, name: opts.name)

  def append(writer, payload), do: GenServer.cast(writer, {:append, payload})

  @impl true
  def init(path), do: {:ok, path}

  @impl true
  def handle_cast({:append, payload}, path) do
    File.write!(path, "#{StampLib.stamp()} #{payload}\n", [:append])
    {:noreply, path}
  end
end
