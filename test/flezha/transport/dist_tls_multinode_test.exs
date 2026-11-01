defmodule Flezha.Transport.DistTlsMultinodeTest do
  @moduledoc """
  The shipped mutual-TLS setup for distribution, `deploy/dist-tls/`: peers run
  distribution with the example options file and certificates the shipped
  script made, and every scenario is a connect between two of them. This node
  keeps its plain distribution and drives the peers over their standard I/O.
  """
  # async: false: starts peer BEAMs registered with the host's EPMD.
  use FlezhaTest.Case, fixtures: [:tmp_dir], async: false

  @moduletag :multi_node

  alias FlezhaTest.Helper.NodeTestHelper

  @script Path.expand("deploy/dist-tls/make-certs.sh")
  @example Path.expand("deploy/dist-tls/dist_tls.conf")

  setup %{tmp: dir} do
    # Lets a peer resolve `a.example`, the host the name check is proved on.
    inetrc = Path.join(dir, "inetrc")
    File.write!(inetrc, ~s({host, {127,0,0,1}, ["a.example"]}.\n{lookup, [file, native]}.\n))

    authority!(dir, "ca_a")
    authority!(dir, "ca_b")
    %{dir: dir, inetrc: inetrc}
  end

  test "two nodes with certificates from one authority and one cookie connect", ctx do
    a = tls_peer(ctx, "a", node_certs!(ctx, "ca_a", "a"))
    b = tls_peer(ctx, "b", node_certs!(ctx, "ca_a", "b"))

    assert connects?(a, b)
    assert :peer.call(a.pid, :erpc, :call, [b.node, :erlang, :node, []]) == b.node
  end

  test "a certificate from another authority is refused", ctx do
    a = tls_peer(ctx, "a", node_certs!(ctx, "ca_a", "a"))

    # It does not check the certificate it is answered with, as an intruder
    # would not, so only the answering node's own check can refuse it.
    foreign =
      tls_peer(ctx, "foreign", node_certs!(ctx, "ca_b", "foreign"),
        edit: &edit(&1, :client, fn opts -> Keyword.put(opts, :verify, :verify_none) end)
      )

    refute connects?(foreign, a)
    refute connects?(a, foreign)
  end

  test "a peer presenting no certificate is refused", ctx do
    a = tls_peer(ctx, "a", node_certs!(ctx, "ca_a", "a"))

    bare =
      tls_peer(ctx, "bare", node_certs!(ctx, "ca_a", "bare"),
        edit: &edit(&1, :client, fn opts -> Keyword.drop(opts, [:certfile, :keyfile]) end)
      )

    refute connects?(bare, a)
  end

  test "a certificate without the client usage is refused", ctx do
    a = tls_peer(ctx, "a", node_certs!(ctx, "ca_a", "a"))
    server_only = tls_peer(ctx, "server_only", server_only_certs!(ctx, "ca_a", "server_only"))

    refute connects?(server_only, a)
  end

  test "a node whose certificate does not name the host it was dialled at is refused", ctx do
    a = tls_peer(ctx, "a", node_certs!(ctx, "ca_a", "a"))

    named =
      tls_peer(ctx, "named", node_certs!(ctx, "ca_a", "named", ["--dns", "a.example"]),
        host: "a.example"
      )

    misnamed =
      tls_peer(ctx, "misnamed", node_certs!(ctx, "ca_a", "misnamed", ["--dns", "b.example"]),
        host: "a.example"
      )

    assert connects?(a, named)
    refute connects?(a, misnamed)
  end

  test "a valid certificate with another cookie is refused", ctx do
    a = tls_peer(ctx, "a", node_certs!(ctx, "ca_a", "a"))
    stranger = tls_peer(ctx, "stranger", node_certs!(ctx, "ca_a", "stranger"), cookie: :other)

    refute connects?(stranger, a)
  end

  test "a peer offering only TLS 1.2 is refused", ctx do
    a = tls_peer(ctx, "a", node_certs!(ctx, "ca_a", "a"))
    tls12 = fn opts -> Keyword.put(opts, :versions, [:"tlsv1.2"]) end

    old =
      tls_peer(ctx, "old", node_certs!(ctx, "ca_a", "old"),
        edit: &(&1 |> edit(:client, tls12) |> edit(:server, tls12))
      )

    refute connects?(old, a)
  end

  test "a node left on the old authority and cookie is cut off by rotating both", ctx do
    a = tls_peer(ctx, "a", node_certs!(ctx, "ca_a", "a"), index: 1)
    b = tls_peer(ctx, "b", node_certs!(ctx, "ca_a", "b"), index: 1)
    c = tls_peer(ctx, "c", node_certs!(ctx, "ca_a", "c"))
    assert connects?(a, b) and connects?(a, c)

    # Two of them are restarted, under the same names, with authority B and a
    # new cookie; the third keeps what it had.
    Enum.each([a, b], &:peer.stop(&1.pid))
    a = tls_peer(ctx, "a", node_certs!(ctx, "ca_b", "a2"), index: 1, cookie: :cluster_b)
    b = tls_peer(ctx, "b", node_certs!(ctx, "ca_b", "b2"), index: 1, cookie: :cluster_b)

    assert connects?(a, b)
    refute connects?(c, a)
    refute connects?(c, b)
  end

  # ---------- peers ----------

  # A peer running distribution over TLS with the example file, its paths moved
  # to `certs`, the folder `make-certs.sh node` wrote. `edit:` changes the
  # file's terms first.
  defp tls_peer(ctx, name, certs, opts \\ []) do
    conf = Keyword.get(opts, :edit, & &1).(example(certs))
    path = Path.join(ctx.dir, "#{name}_#{System.unique_integer([:positive])}.conf")
    File.write!(path, :io_lib.format("~p.~n", [conf]))

    args = [
      ~c"-proto_dist",
      ~c"inet_tls",
      ~c"-ssl_dist_optfile",
      String.to_charlist(path),
      ~c"-kernel",
      ~c"inetrc",
      String.to_charlist(inspect(ctx.inetrc))
    ]

    {pid, node} =
      NodeTestHelper.start_bare_peer(
        "dist_tls_#{name}",
        Keyword.get(opts, :index, System.unique_integer([:positive])),
        args,
        connection: :standard_io,
        cookie: Keyword.get(opts, :cookie, :cluster_a),
        host: Keyword.get(opts, :host, "127.0.0.1")
      )

    %{pid: pid, node: node}
  end

  defp connects?(from, to),
    do: :peer.call(from.pid, :net_kernel, :connect_node, [to.node], 30_000)

  defp example(certs) do
    {:ok, [conf]} = :file.consult(@example)

    for {side, opts} <- conf do
      {side,
       Enum.map(opts, fn
         {key, file} when key in [:certfile, :keyfile, :cacertfile] ->
           {key, file |> to_string() |> String.replace("/etc/flezha/dist", certs)}

         option ->
           option
       end)}
    end
  end

  defp edit(conf, side, fun), do: List.keyreplace(conf, side, 0, {side, fun.(conf[side])})

  # ---------- certificates ----------

  defp authority!(dir, name), do: script!(["ca", Path.join(dir, name)])

  # The node folder `make-certs.sh` writes, naming 127.0.0.1 unless `san` says.
  defp node_certs!(ctx, authority, name, san \\ ["--ip", "127.0.0.1"]) do
    script!(["node", Path.join(ctx.dir, authority), name | san])
    Path.join([ctx.dir, authority, name])
  end

  defp script!(args) do
    {out, status} = System.cmd(@script, args, stderr_to_stdout: true)
    assert status == 0, out
  end

  # What the script never makes: a leaf of the authority carrying serverAuth
  # alone, laid out as a node folder.
  defp server_only_certs!(ctx, authority, name) do
    ca = Path.join(ctx.dir, authority)
    out = Path.join(ca, name)
    File.mkdir_p!(out)
    ext = Path.join(out, "ext")
    File.write!(ext, "extendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1\n")

    sh!("""
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "#{out}/node.key" &&
    openssl req -new -key "#{out}/node.key" -subj "/CN=#{name}" |
      openssl x509 -req -CA "#{ca}/ca.pem" -CAkey "#{ca}/ca.key" -set_serial 7 -days 1 \
        -extfile "#{ext}" -out "#{out}/node.pem" &&
    cp "#{ca}/ca.pem" "#{out}/ca.pem"
    """)

    out
  end

  defp sh!(script) do
    {out, status} = System.cmd("sh", ["-c", script], stderr_to_stdout: true)
    assert status == 0, out
  end
end
