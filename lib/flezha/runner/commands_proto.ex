defmodule Flezha.Runner.CommandsProto do
  @moduledoc """
  The generated `RunnerCommands` protobuf module set.

  `use Protox` compiles `crates/protobufs/runner_commands.proto` into top-level
  `RunnerCommands.*` structs; every controller/SDK control frame is one of
  them. Invariant: this file declares the source and nothing else — a field
  is added to the `.proto`, never here.
  """

  use Protox,
    files: [
      "./crates/protobufs/runner_commands.proto"
    ]
end
