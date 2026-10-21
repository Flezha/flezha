defmodule Flezha.State.DurableFrameConformanceTest do
  @moduledoc """
  The durable record frame, driven from the bytes in
  `test/fixtures/durable_frame.txt` — the file `crates/flz_core/src/restore.rs`'s tests
  read too.

  Every reader of the framing is folded over the same golden log here: the
  encoder that writes it, the byte walk, the device walk, the single-record
  decode. A reader that decomposes the frame its own way answers differently on
  one of these rows, which is the point of keeping the bytes in a file rather
  than in each test.
  """
  use FlezhaTest.Case, fixtures: [{:tmp_dir, base: Path.expand("storage/tmp")}], async: true

  import ExUnit.CaptureLog

  alias Flezha.State.Frame
  alias Flezha.State.{RecordFrame, TestHelpers}
  alias FlezhaTest.Helper.WireEncoders

  @fixture Path.join([__DIR__, "..", "..", "fixtures", "durable_frame.txt"])
  @external_resource @fixture

  # The three bodies every `frame` row is built from, and the stamp they carry.
  @bodies [{0, <<1, 2, 3>>}, {1, <<4, 5>>}, {2, <<>>}]
  @lang_rust 1
  @sdk_version 65_536
  @schema_version 1

  defp rows(kind) do
    @fixture
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Enum.map(&(&1 |> String.split("|") |> Enum.map(fn f -> String.trim(f) end)))
    |> Enum.filter(&match?([^kind | _], &1))
    |> Enum.map(&tl/1)
    |> tap(&assert &1 != [], "the fixture holds no #{kind} rows")
  end

  defp bytes("-"), do: <<>>
  defp bytes(hex), do: Base.decode16!(hex, case: :lower)

  defp seqs("-"), do: []
  defp seqs(list), do: list |> String.split(",") |> Enum.map(&String.to_integer/1)

  defp collect({seq, _header, _body, _raw}, acc), do: {:cont, [seq | acc]}

  describe "the frame both runtimes read" do
    test "the encoder produces the fixture's bytes" do
      # The write side is pinned by the same file the read side is: a change to
      # `encode_record/3` that no reader notices still fails here.
      log =
        Enum.map_join(@bodies, "", fn {seq, body} ->
          header =
            WireEncoders.encode_header(@lang_rust, @sdk_version, @schema_version, 1, body)

          Frame.encode_record(seq, header, body)
        end)

      assert Base.encode16(log, case: :lower) ==
               rows("frame")
               |> Enum.find_value(fn [name, hex | _] -> name == "three_records" && hex end)
    end

    test "the byte walk reaches the fixture's verdict on every row" do
      Enum.each(rows("frame"), fn [name, hex, verdict, seqs] ->
        log = bytes(hex)

        result =
          capture_result(fn ->
            Frame.fold_bytes(log, {:payload, "fixture:#{name}"}, [], &collect/2)
          end)

        case verdict do
          "ok" ->
            assert {:ok, folded} = result, "row #{name}"
            assert Enum.reverse(folded) == seqs(seqs), "row #{name}"

          reason ->
            assert {:error, {refused, _last_seq, _offset}} = result, "row #{name}"
            assert Atom.to_string(refused) == reason, "row #{name}"
        end
      end)
    end

    test "the device walk reaches the same verdict over the same bytes", %{tmp: tmp} do
      Enum.each(rows("frame"), fn [name, hex, verdict, seqs] ->
        path = Path.join(tmp, "#{name}.log")
        File.write!(path, bytes(hex))

        result = capture_result(fn -> TestHelpers.record_seqs(path) end)

        # A device log MAY have a torn tail, so its answer to a crc failure with
        # a record behind it is the same refusal and no other row differs.
        case verdict do
          "ok" ->
            assert result == {:ok, seqs(seqs)}, "row #{name}"

          reason ->
            assert {:error, {refused, _last_seq, _offset}} = result, "row #{name}"
            assert Atom.to_string(refused) == reason, "row #{name}"
        end
      end)
    end

    test "one record decoded from bytes and from a device is the same record", %{tmp: tmp} do
      hex = Enum.find_value(rows("frame"), fn [n, hex | _] -> n == "three_records" && hex end)
      log = bytes(hex)
      path = Path.join(tmp, "three.log")
      File.write!(path, log)

      {:ok, device} = :file.open(path, [:raw, :read, :binary])

      try do
        assert {:ok, seq, header, body, raw, consumed} = Frame.decode_bytes(log)
        assert {:ok, ^seq, ^header, ^body, ^consumed} = RecordFrame.decode_record(device, 0)
        assert raw == binary_part(log, 0, consumed)
      after
        :file.close(device)
      end
    end
  end

  describe "the header table both runtimes hold" do
    test "every allocated value decodes to the fields the fixture declares" do
      Enum.each(rows("header"), fn [name, hex, lang, version, schema, format, crc] ->
        assert {:ok, decoded} = Frame.decode_header(bytes(hex)), "row #{name}"

        assert decoded == %{
                 sdk_lang: String.to_integer(lang),
                 sdk_version: String.to_integer(version),
                 schema_version: String.to_integer(schema),
                 format_id: String.to_integer(format),
                 crc32: String.to_integer(crc, 16)
               },
               "row #{name}"
      end)
    end

    test "a header of any other length is refused rather than guessed at" do
      assert :error = Frame.decode_header(<<1, 2, 3>>)
      assert :error = Frame.decode_header(<<>>)
    end

    test "the language table names every allocated value" do
      Enum.each(rows("lang"), fn [value, name] ->
        assert Frame.sdk_lang_name(String.to_integer(value)) == name
      end)

      assert Frame.sdk_lang_name(200) == "unallocated"
    end

    test "the format table is the one the SDK stamps with" do
      Enum.each(rows("header"), fn [name, hex, _lang, _version, _schema, format, _crc] ->
        if String.starts_with?(name, "format_") do
          codec = name |> String.trim_leading("format_") |> String.to_atom()
          assert Frame.format_id(codec) == String.to_integer(format)
          assert {:ok, %{format_id: id}} = Frame.decode_header(bytes(hex))
          assert id == String.to_integer(format)
        end
      end)
    end
  end

  # A fold that refuses logs at `error`; the verdict is what is under test, not
  # the line, so it is captured wherever the row is a refusal.
  defp capture_result(fun) do
    ref = make_ref()

    capture_log(fn -> send(self(), {ref, fun.()}) end)

    receive do
      {^ref, result} -> result
    after
      0 -> flunk("the fold did not answer")
    end
  end
end
