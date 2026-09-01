defmodule Flezha.Schedule.CronTest do
  use ExUnit.Case, async: true

  alias Flezha.Schedule.Cron

  describe "parse/1" do
    test "rejects a wrong field count" do
      assert {:error, msg} = Cron.parse("* * * *")
      assert msg =~ "5 fields"
    end

    test "rejects out-of-range and malformed elements, naming the field" do
      assert {:error, msg} = Cron.parse("60 * * * *")
      assert msg =~ "minute field"

      assert {:error, msg} = Cron.parse("* 24 * * *")
      assert msg =~ "hour field"

      assert {:error, msg} = Cron.parse("* * 0 * *")
      assert msg =~ "day-of-month field"

      assert {:error, _} = Cron.parse("a * * * *")
      assert {:error, _} = Cron.parse("5-1 * * * *")
      assert {:error, _} = Cron.parse("*/0 * * * *")
    end

    test "rejects a step attached to a single value" do
      assert {:error, msg} = Cron.parse("30/10 * * * *")
      assert msg =~ "minute field"
      # An explicit `/1` is still a stepped single value, not a bare `30`.
      assert {:error, msg} = Cron.parse("30/1 * * * *")
      assert msg =~ "minute field"
      assert {:error, msg} = Cron.parse("* 5/2 * * *")
      assert msg =~ "hour field"

      assert {:ok, cron} = Cron.parse("30-59/10 * * * *")
      assert Enum.sort(cron.minutes) == [30, 40, 50]
    end

    test "expands lists, ranges and steps" do
      assert {:ok, cron} = Cron.parse("*/15 1,2 10-12 * *")
      assert Enum.sort(cron.minutes) == [0, 15, 30, 45]
      assert Enum.sort(cron.hours) == [1, 2]
      assert Enum.sort(cron.days) == [10, 11, 12]
      assert Enum.sort(cron.months) == Enum.to_list(1..12)
    end

    test "weekday 7 normalizes to Sunday" do
      assert {:ok, cron} = Cron.parse("0 0 * * 7")
      assert MapSet.member?(cron.weekdays, 0)
    end
  end

  describe "valid_timezone?/1" do
    test "accepts IANA zones and rejects unknown ones" do
      assert Cron.valid_timezone?("Europe/Tallinn")
      assert Cron.valid_timezone?("Etc/UTC")
      refute Cron.valid_timezone?("Not/AZone")
    end
  end

  describe "next_fire/3" do
    test "finds the next slot, strictly after the origin" do
      {:ok, cron} = Cron.parse("0 8 * * *")

      assert {:ok, ~U[2026-07-27 08:00:00Z]} =
               Cron.next_fire(cron, "Etc/UTC", ~U[2026-07-27 07:00:00Z])

      assert {:ok, ~U[2026-07-28 08:00:00Z]} =
               Cron.next_fire(cron, "Etc/UTC", ~U[2026-07-27 08:00:00Z])
    end

    test "evaluates in the entry's timezone" do
      {:ok, cron} = Cron.parse("0 8 * * *")

      # 08:00 Europe/Tallinn in July is EEST (UTC+3) = 05:00 UTC.
      assert {:ok, ~U[2026-07-27 05:00:00Z]} =
               Cron.next_fire(cron, "Europe/Tallinn", ~U[2026-07-27 00:00:00Z])
    end

    test "vixie rule: restricted day-of-month OR day-of-week" do
      {:ok, cron} = Cron.parse("0 0 14 * 5")

      # 13 Feb 2026 is a Friday — fires before the 14th.
      assert {:ok, ~U[2026-02-13 00:00:00Z]} =
               Cron.next_fire(cron, "Etc/UTC", ~U[2026-02-09 00:00:00Z])

      assert {:ok, ~U[2026-02-14 00:00:00Z]} =
               Cron.next_fire(cron, "Etc/UTC", ~U[2026-02-13 00:00:00Z])
    end

    test "a spring-forward gap slot is skipped" do
      {:ok, cron} = Cron.parse("30 3 * * *")

      # Europe/Tallinn, 29 Mar 2026: 03:00-04:00 local does not exist, so the
      # 29th's slot skips to the 30th (03:30 EEST = 00:30 UTC).
      assert {:ok, ~U[2026-03-30 00:30:00Z]} =
               Cron.next_fire(cron, "Europe/Tallinn", ~U[2026-03-29 00:00:00Z])
    end

    test "an ambiguous fall-back slot fires once, at the first occurrence" do
      {:ok, cron} = Cron.parse("30 3 * * *")

      # Europe/Tallinn, 25 Oct 2026: 03:00-04:00 local occurs twice; the slot is
      # the first pass (EEST, UTC+3) and the second pass never fires.
      assert {:ok, ~U[2026-10-25 00:30:00Z] = first} =
               Cron.next_fire(cron, "Europe/Tallinn", ~U[2026-10-25 00:00:00Z])

      assert {:ok, ~U[2026-10-26 01:30:00Z]} = Cron.next_fire(cron, "Europe/Tallinn", first)
    end

    test "a never-matching expression returns :none" do
      {:ok, cron} = Cron.parse("0 0 30 2 *")
      assert :none = Cron.next_fire(cron, "Etc/UTC", ~U[2026-07-27 00:00:00Z])
    end

    test "a leap-day expression is found within the scan horizon" do
      {:ok, cron} = Cron.parse("0 0 29 2 *")

      assert {:ok, ~U[2028-02-29 00:00:00Z]} =
               Cron.next_fire(cron, "Etc/UTC", ~U[2026-07-27 00:00:00Z])
    end
  end

  describe "slots_between/5" do
    test "returns slots oldest-first within the half-open interval" do
      {:ok, cron} = Cron.parse("* * * * *")

      assert {[~U[2026-07-27 08:01:00Z], ~U[2026-07-27 08:02:00Z], ~U[2026-07-27 08:03:00Z]],
              false} =
               Cron.slots_between(
                 cron,
                 "Etc/UTC",
                 ~U[2026-07-27 08:00:30Z],
                 ~U[2026-07-27 08:03:00Z],
                 10
               )
    end

    test "reports truncation at the limit" do
      {:ok, cron} = Cron.parse("* * * * *")

      assert {[_, _], true} =
               Cron.slots_between(
                 cron,
                 "Etc/UTC",
                 ~U[2026-07-27 08:00:30Z],
                 ~U[2026-07-27 09:00:00Z],
                 2
               )
    end
  end
end
