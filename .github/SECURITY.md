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

## Before you report

A controller with authorization turned off takes every command on trust; that
is how the dev and test configuration runs, and it is not a vulnerability.
[Authorization](../docs/public/topics/core/authorization.md) explains the
postures and what a production controller should use.
