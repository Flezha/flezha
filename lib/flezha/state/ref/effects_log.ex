defmodule Flezha.State.Ref.EffectsLog do
  @moduledoc "`{:effects_log, class}`: an effect log, packed whole (a cut renumbers it)."

  use Flezha.State.Ref

  alias Flezha.State.{Paths, Ref}

  def files({:effects_log, class}), do: [Paths.effects_log_path(class)]

  def payload_files({:effects_log, class}),
    do: Ref.present(Paths.class_dir(class), ["effects.log"])
end
