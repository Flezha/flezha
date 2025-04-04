defprotocol Aflecha.Plugin.PipelineBinding do
  @spec bind(t()) :: {:ok, t()} | {:error, String.t()}
  def bind(settings)

  @spec exec(
          t(),
          {:binary | :text, binary()}
        ) :: {:ok, {:binary | :text, binary()}} | {:error, String.t()}
  def exec(settings, payload)
end

defmodule Aflecha.Plugin.PipelinePlugin do
  def default_keys() do
    [:pipeline_id, :action_mode]
  end
end



defmodule CommonProtocol do
  defmacro __using__(_) do
    quote do
      @doc "A common function signature"
      def common_function(data)
    end
  end
end

defprotocol MyProtocol do
  use CommonProtocol  # Injects only the function signature
  @doc "A function defined in MyProtocol"
  def my_function(data)
end

defprotocol ExtendedProtocol do
  use CommonProtocol  # Injects only the function signature
  @doc "An extended function"
  def extended_function(data)
end

defimpl MyProtocol, for: Integer do
  def my_function(data), do: "MyProtocol implementation for #{data}"
  def common_function(data), do: "MyProtocol: Common function for #{data}"
end

defimpl ExtendedProtocol, for: Integer do
  def extended_function(data), do: "Extended function for #{data}"
  def common_function(data), do: "ExtendedProtocol: Common function for #{data}"
end
