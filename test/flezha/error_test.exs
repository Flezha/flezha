defmodule Flezha.ErrorTest do
  use ExUnit.Case, async: true

  alias Flezha.Error

  @fixture "test/fixtures/error_rendering.json" |> File.read!() |> Jason.decode!()

  test "every case of the shared rendering fixture renders the bytes the Rust renderer does" do
    for %{"code" => code, "details" => details, "text" => text, "json" => json} <- @fixture do
      error = Error.new(code, details)
      assert Error.render(error, :text) == text, code
      assert Error.render(error, :json) == json, code
    end
  end

  test "a reason maps to its code, its arguments naming the details" do
    error = Error.from_reason({:quota_exceeded, :max_operators, %{current: 3, limit: 3}}, nil)

    assert %Error{code: "quota.exceeded", class: :conflict, hint: "flz namespace show"} = error
    assert error.details == %{"kind" => "max_operators", "current" => 3, "limit" => 3}
    assert error.message == "the namespace's max_operators quota is reached (3 of 3)"
  end

  test "an entry limited to a site applies there and only there" do
    assert %Error{code: "namespace.not_found"} =
             Error.from_reason({:not_found, "ghost"}, :"authz.namespace_delete")

    Process.put(:flezha_uncatalogued_expected, true)

    assert %Error{code: "internal.uncatalogued"} =
             Error.from_reason({:not_found, "ghost"}, :"authz.token_list")
  end

  test "an error is answered as it is" do
    error = Error.new("namespace.not_found", namespace: "ghost")
    assert Error.from_reason(error, :anywhere) == error
  end

  test "an unmapped reason is uncatalogued, carries the term as text, and says so on telemetry" do
    ref = make_ref()
    test = self()

    :telemetry.attach(
      "error-test-#{inspect(ref)}",
      [:flezha, :error, :uncatalogued],
      fn _event, _measurements, metadata, _ -> send(test, {ref, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("error-test-#{inspect(ref)}") end)
    Process.put(:flezha_uncatalogued_expected, true)

    error = Error.from_reason({:brand_new_reason, %{a: 1}}, "wire.spawn")

    assert %Error{code: "internal.uncatalogued", class: :internal} = error
    assert error.details == %{"reason" => "{:brand_new_reason, %{a: 1}}"}
    assert_receive {^ref, %{reason: {:brand_new_reason, %{a: 1}}, site: "wire.spawn"}}
  end

  test "every class exits with its own code" do
    exits =
      for class <- [
            :internal,
            :invalid,
            :unreachable,
            :denied,
            :not_found,
            :conflict,
            :unavailable
          ],
          do: Error.exit_code(%Error{class: class})

    assert exits == [1, 2, 3, 4, 5, 6, 7]
  end

  test "a detail that is not plain data is carried as text" do
    error = Error.new("namespace.not_found", namespace: {:a, self()})
    assert error.details["namespace"] =~ "{:a, #PID<"
  end
end
