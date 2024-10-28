defmodule Aflecha.Service.Validator do
  def byte_limit_exceed(value, max_length) do
    byte_size(value) > max_length
  end
end
