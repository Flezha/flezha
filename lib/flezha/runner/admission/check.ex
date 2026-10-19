defmodule Flezha.Runner.Admission.Check do
  @moduledoc "One check of `Flezha.Runner.Admission`."

  @callback check(connect :: map(), context :: map()) :: :ok | {:ok, map()} | {:error, term()}
end
