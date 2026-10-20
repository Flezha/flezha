defmodule Flezha.ErrorTest do
  use ExUnit.Case, async: true

  alias Flezha.Error
  alias FlezhaTest.Helper.Telemetry

  @fixture "test/fixtures/error_rendering.json" |> File.read!() |> Jason.decode!()

  test "every case of the shared rendering fixture renders the bytes the Rust renderer does" do
    for %{"code" => code, "details" => details, "text" => text, "json" => json} <- @fixture do
      error = Error.new(code, details)
      assert Error.render(error, :text) == text, code
      assert Error.render(error, :json) == json, code
    end
  end

  test "a reason maps to its code, its arguments naming the details" do
    error = Error.from_reason({:quota_exceeded, :max_relays, %{current: 3, limit: 3}}, nil)

    assert %Error{code: "quota.exceeded", class: :conflict, hint: "flz namespace show"} = error
    assert error.details == %{"kind" => "max_relays", "current" => 3, "limit" => 3}
    assert error.message == "the namespace's max_relays quota is reached (3 of 3)"
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

    Telemetry.capture([:flezha, :error, :uncatalogued],
      as: fn _event, _measurements, metadata -> {ref, metadata} end
    )

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

  # Fails when: a controller term or an internal reason reaches a networked
  # caller, the log loses it, or a detail written for the caller is withheld.
  test "a networked caller reads no term of the controller's own, and the log keeps it" do
    internal = Error.new("relay.start_failed", relay: "o1", reason: "vault key s3cr3t-7f3a")
    term = Error.new("commit.turn_not_parked", reason: {:exit, "vault key s3cr3t-7f3a"})
    pointer = ~r/^the controller's log names the cause \(ref [0-9a-f]{8}\)$/

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert %{"relay" => "o1", "reason" => door} = Error.withheld(internal, "door").details
        assert %{"reason" => wire} = Error.withheld(term, "wire").details
        assert door =~ pointer and wire =~ pointer
      end)

    assert log =~ "door: " and log =~ "wire: "
    assert length(String.split(log, "s3cr3t-7f3a")) == 3

    unknown = %Error{code: "runtime.newer", class: :internal, details: %{"reason" => "x"}}
    others = [Error.new("call.unavailable", relay: "o1", reason: "parked"), unknown]
    assert Enum.map(others, &Error.withheld(&1, "door")) == others
  end

  # Fails when: the pointer a caller reads names a line other than its own, in
  # the log's text or its `log_ref` metadata.
  test "a withheld reason's pointer id is the one its log line carries" do
    error = Error.new("relay.start_failed", relay: "o1", reason: "boom")

    {withheld, log} =
      ExUnit.CaptureLog.with_log([format: "$metadata| $message", metadata: [:log_ref]], fn ->
        Error.withheld(error, "door")
      end)

    assert [_, ref] = Regex.run(~r/names the cause \(ref (\w{8})\)$/, withheld.details["reason"])
    assert log =~ "log_ref=#{ref} | door: relay.start_failed "
    assert log =~ ~s|{"reason":"boom","relay":"o1"} (ref #{ref})|
  end

  test "a detail that is not plain data is carried as text" do
    error = Error.new("namespace.not_found", namespace: {:a, self()})
    assert error.details["namespace"] =~ "{:a, #PID<"
  end
end
