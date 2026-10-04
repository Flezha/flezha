# Contributing

Bug reports, documentation fixes and code are all welcome. This page has the
rules a contribution follows. The pages after it cover the practical side:

| Page | What it covers |
|---|---|
| [Setting up a checkout](setting_up.md) | The tools, the clone, the build, and the development containers |
| [Running the tests](testing.md) | The test suites, one SDK at a time, and running CI on your own machine |
| [Code standards](code_standards.md) | The rules a change is held to, the formatters and the gates |
| [Releasing](releasing.md) | The artifacts a release publishes and how they are built |

## Before you start

- **Report a security issue privately.** Never in a public issue:
  [`SECURITY.md`](https://github.com/flezha/flezha/blob/main/SECURITY.md) says
  where to send it.
- **Open an issue before a large change.** A new feature, a change to the wire
  protocol or to what every SDK carries, or a rework of a part of the
  controller is easier to agree on before the code exists. A small fix needs no
  issue first.
- **Sign the CLA.** Every contribution needs a signed
  [Individual CLA](https://github.com/flezha/flezha/blob/main/CLA.md). The
  cla-assistant bot asks for it on your first pull request, and one signature
  covers every later one. A contribution is licensed under the license of the
  path it lands in, MPL-2.0 or Apache-2.0, as
  [`REUSE.toml`](https://github.com/flezha/flezha/blob/main/REUSE.toml) maps
  them.

## A pull request

- **One concern per pull request.** A fix does not carry an unrelated
  refactoring, and a reformat of code you did not otherwise change goes in a
  pull request of its own.
- **Tests come with the change.** A bug fix comes with a test that fails without
  it. New behaviour comes with tests that show it working.
- **Documentation comes with the change.** When a change alters what a user
  sees, such as a command, an option, a default or an error, the pages that
  describe it change in the same pull request.
- **One change, every SDK.** Anything every SDK carries (declarations, the wire
  protocol, defaults, the shared test fixtures) changes in every SDK in the same
  change, or the change says what it leaves for later.
  [`docs/architecture/runner.md`](https://github.com/flezha/flezha/blob/main/docs/architecture/runner.md#one-change-every-sdk)
  has the rule in full.
- **No build output in git.** Native libraries, jars and release archives are
  built, never committed, and a gate fails a pull request that adds one.
- **Run the checks first.** [Code standards](code_standards.md#the-gates) and
  [Running the tests](testing.md) list what CI runs and how to run the same on
  your machine.
- **Everything in the repository is in English:** code, comments,
  documentation and commit messages.

## Commit messages

A commit message is read by someone scanning the history a year from now:

- A subject line of about 50 characters that says what the change does, such
  as `Retry a failed plugin start once`.
- A blank line, then a short paragraph or two on what changed and why. The
  details belong in the code and its tests, not in the message.
- No generated trailers and no pasted command output.

## Review

A maintainer reviews every pull request, and CI must be green before it is
merged. Review comments are about the change, not the person, and a pull
request that has been asked for changes stays open until they are made or the
discussion settles them.
