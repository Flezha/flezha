defmodule Flezha.Processor.InitStatePlugin do
  @moduledoc """
  Declarative configuration for a single init-state provider in a processor's
  startup chain.

  Plugins are set on a processor via
  `Flezha.Processor.Builder.opts[:init_state_plugins]` as an ordered list of
  `%Flezha.Processor.InitStatePlugin{}` structs. See
  `Flezha.Processor.InitStateProvider` for the provider contract and
  `Flezha.Processor.InitStateResolver` for the resolution semantics.
  """

  @type on_error :: :fail | :empty

  @type t() :: %__MODULE__{
          provider: module(),
          opts: map(),
          on_error: on_error(),
          timeout_ms: pos_integer()
        }

  @enforce_keys [:provider]
  defstruct [
    :provider,
    opts: %{},
    on_error: :fail,
    timeout_ms: 5_000
  ]

  @doc """
  Build an `%InitStatePlugin{}` from a keyword list.

  Raises `ArgumentError` when `:provider` is missing. Other fields fall back
  to their struct defaults.
  """
  @spec new(Keyword.t()) :: t()
  def new(opts) when is_list(opts) do
    unless Keyword.has_key?(opts, :provider) do
      raise ArgumentError,
            "Flezha.Processor.InitStatePlugin.new/1 requires :provider — got #{inspect(opts)}"
    end

    struct!(__MODULE__, opts)
  end
end
