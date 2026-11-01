# Security policy

## Reporting a vulnerability

Please do not report security problems in public issues, pull requests or
discussions.

Report them privately through GitHub: open the repository's **Security** tab
and choose **Report a vulnerability**. Only the maintainers can see the report.
The same applies to the SDK and plugin repositories: report a problem in the
repository it belongs to, or here if you are not sure.

A useful report says:

- which component is affected: the controller, `flz`, an SDK or a plugin, and
  its version
- what an attacker can do, and what they need first (network access, a token,
  a runner credential)
- the steps or a small program that shows it

You will get an answer within seven days. Once the problem is confirmed, it is
fixed in a new release and described in its changelog and in a GitHub security
advisory, which credits you unless you prefer not to be named.

## Supported versions

Flezha is pre-1.0. Fixes go into the next release only; earlier releases are
not patched. Run the latest release.

## Security model

Controllers on separate machines form one cluster over Erlang distribution.
Distribution has no permissions: a node connected to the cluster can run any
code on every other node. What decides who may connect is the whole boundary,
and it is two things:

- **The cookie**, `RELEASE_COOKIE`, which every node of the cluster shares.
- **Mutual TLS**, when `FLEZHA_DIST_TLS_OPTFILE` names an options file: each
  node presents a certificate from a distribution authority and checks the
  other's. Without it, distribution is plain TCP, readable and alterable by
  anyone on the network path, and the controller warns about it on every boot.

Inside that boundary, everything else is scoped:

- **Runners** connect over the runner transport (a Unix socket, or QUIC with
  mutual TLS) and act only in the namespace their credential or certificate
  proves.
- **Tokens** carry a principal and a role in a namespace, and every command is
  checked against them when authorization is on.
- **Namespaces** separate tenants: their relays, plugins, files and roles.
- **The console** is a hidden Erlang node holding the cookie, so it sits inside
  the boundary: whoever controls the console machine controls the cluster.

## Hardening a production cluster

- Provision `RELEASE_COOKIE` as a secret, the same on every node, and never
  reuse it elsewhere.
- Run distribution over mutual TLS with a certificate authority used for
  nothing else: `deploy/dist-tls/` has the options file and a script that makes
  the authority and node certificates.
- Keep `{versions, ['tlsv1.3']}` in that file; without it only TLS 1.2 is
  offered.
- Set `FLEZHA_DIST_PORT` so the distribution port is fixed, and allow port 4369
  and that port only from the other controllers' and the console's IP addresses.
- Bind EPMD and distribution to a private address (`ERL_EPMD_ADDRESS`,
  `-kernel inet_dist_use_interface`), for example on a WireGuard network.
- On Kubernetes, apply `deploy/kubernetes/flezha-networkpolicy.yaml` on a
  network plugin that enforces it, and `deploy/dist-tls/kubernetes/` for mTLS.
- Run the console on a machine you trust as much as the controllers, and give
  it its own certificate from the distribution authority.
- Treat crash dumps (`erl_crash.dump`) as secrets: they hold the cookie in
  clear text.
- To cut a node off, rotate the cookie and the authority on every other node and
  restart them; rebuild what the removed node could reach.
- Run the latest controller image; it is rebuilt when the Erlang runtime gets a
  security fix.

## Before you report

A controller with authorization turned off takes every command on trust; that
is how the dev and test configuration runs, and it is not a vulnerability.
Neither is what a node that holds the cookie and passes the TLS check can do,
the console included: the model above places it inside the boundary.
[Authorization](../docs/public/topics/core/authorization.md) explains the
postures and what a production controller should use.
