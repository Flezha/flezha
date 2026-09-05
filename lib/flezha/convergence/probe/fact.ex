defmodule Flezha.Convergence.Probe.Fact do
  @moduledoc """
  One observation about one subject — the shape every detector's fact reader
  returns, and the only thing a reader is allowed to produce.

  `observed` is a flat map of raw fields; the judge merges every fact about one
  subject and decides from the merged map. A reader that returned a verdict
  instead of an observation would be deciding, which is what the seam exists to
  stop, so the struct carries no state word at all — and no threshold either:
  the window a verdict was judged by is named by the judge, from the reason it
  decided on, so a reader cannot cite one that did not decide anything.
  """

  @type t :: %__MODULE__{
          subject: {Flezha.Convergence.Verdict.kind(), term()},
          kind: Flezha.Convergence.Verdict.kind(),
          observed: map()
        }

  defstruct subject: nil, kind: nil, observed: %{}

  @doc "Build a fact about `subject`; `kind` is read from the subject itself."
  @spec new({atom(), term()}, map()) :: t()
  def new({kind, _id} = subject, observed),
    do: %__MODULE__{subject: subject, kind: kind, observed: observed}
end
