# file-plugin-s3 Specification

## Purpose
How the file plugins work under an `s3://<bucket>/<prefix>` root: every path confined to the prefix, connection settings kept apart from durable state, one object read by ranged requests, and parts committed with a guarded progress sidecar.
## Requirements
### Requirement: An S3 root SHALL confine every file plugin path to one bucket prefix

The controller SHALL accept `FLEZHA_FILE_PLUGIN_ROOT` in the form `s3://<bucket>/<prefix>`. Each
namespace SHALL have its own prefix, `<prefix>/<namespace>`, and a plugin's `path` SHALL then
name a key under the prefix of the namespace it is enabled in: a relative path SHALL be joined to
that prefix with its `.` and `..` segments removed lexically, an `s3://` path SHALL be taken as
written, and a path starting with `/` SHALL be refused naming the root. The resulting key SHALL
lie under the namespace prefix, compared segment by segment, or the enable SHALL be refused naming
it. The resolved `s3://` URL SHALL replace `path` in the plugin's options. The bucket SHALL NOT be
created.

#### Scenario: A relative path becomes a key under the prefix

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "out/echo"`
- **THEN** the enable SHALL succeed and the plugin's resolved path SHALL be
  `s3://events/flezha/orders/out/echo`

#### Scenario: A path that climbs out is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "../other/echo"`
- **THEN** the enable SHALL be refused naming `s3://events/flezha/orders`

#### Scenario: Another bucket is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled with `path: "s3://audit/flezha/echo"`
- **THEN** the enable SHALL be refused naming the namespace prefix under `s3://events/flezha`

#### Scenario: A filesystem path is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled with `path: "/etc/passwd"`
- **THEN** the enable SHALL be refused naming `s3://events/flezha`

#### Scenario: Another namespace's prefix is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** a plugin is enabled in namespace `payments` with
  `path: "s3://events/flezha/orders/out/echo"`
- **THEN** the enable SHALL be refused naming `s3://events/flezha/payments`

### Requirement: An S3 root SHALL use its own connection settings and SHALL NOT reach durable state

The controller SHALL declare four configuration rows: `FLEZHA_FILE_PLUGIN_S3_ENDPOINT`,
`FLEZHA_FILE_PLUGIN_S3_REGION` (default `us-east-1`), `FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID`, and
`FLEZHA_FILE_PLUGIN_S3_SECRET_ENV`, which names the environment variable holding the secret key.
A file plugin enabled under an S3 root while the endpoint, the access key id or the secret
variable's name is unset SHALL be refused naming the unset variable. The secret SHALL NOT appear
in a log line, a plugin option, a health verdict, the boot line or `flz controller status`.

When the durability tier's backend is `s3` on the same endpoint and bucket as the root, and
either prefix contains the other, every file plugin enable SHALL be refused naming both
prefixes.

#### Scenario: A missing setting is refused naming it

- **GIVEN** the root is `s3://events/flezha` and `FLEZHA_FILE_PLUGIN_S3_ENDPOINT` is unset
- **WHEN** a file plugin is enabled
- **THEN** the enable SHALL be refused naming `FLEZHA_FILE_PLUGIN_S3_ENDPOINT`

#### Scenario: The durability tier's prefix is refused

- **GIVEN** the durability tier stores under bucket `events`, prefix `flezha`, on the same
  endpoint
- **AND** the root is `s3://events/flezha/files`
- **WHEN** a file plugin is enabled
- **THEN** the enable SHALL be refused naming both `flezha` and `flezha/files`

#### Scenario: The secret is never printed

- **GIVEN** the secret variable holds `s3cr3t-value`
- **WHEN** `flz controller status` is read and a request fails with status `403`
- **THEN** neither the status output, the logged failure nor the health verdict SHALL contain
  `s3cr3t-value`

### Requirement: A file source under an S3 root SHALL read one object by ranged requests and resume at its committed offset

While caught up, the reader SHALL ask the object's size and ETag every `poll_interval_ms`. It
SHALL read new bytes in ranged requests of at most `read_chunk_bytes`, each conditioned on the
ETag it read, and SHALL inject and commit lines exactly as `file-source-provider` requires for a
file. An object whose ETag changes, or whose ranged read fails its condition, SHALL be read
again from byte 0, and so SHALL an object shorter than the offset. A missing object SHALL report
`{:unhealthy, {:file_missing, path}}`, keep the offset, and recover when the object appears. A
request failing with any status other than `404` SHALL keep the offset and report a failure
naming the status; no line SHALL be skipped.

#### Scenario: An object's lines become frames in order

- **GIVEN** the object at the plugin's path holds `a\nb\n`
- **WHEN** the reader runs
- **THEN** frames `a` then `b` SHALL be injected and the committed offset SHALL reach 4

#### Scenario: A replaced object is read from the start

- **GIVEN** a reader whose committed offset is the end of `a\nb\n`
- **WHEN** the object is replaced by one holding `c\n`
- **THEN** frame `c` SHALL be injected, read from byte 0

#### Scenario: An object replaced during a read is read from the start

- **GIVEN** a ranged read in progress against ETag `e1`
- **WHEN** the object is replaced and the read fails its condition
- **THEN** the reader SHALL read the new object from byte 0

#### Scenario: A missing object is unhealthy and recovers

- **WHEN** the object at the plugin's path does not exist
- **THEN** health SHALL be `{:unhealthy, {:file_missing, path}}`
- **AND** once the object appears, its lines SHALL be delivered and health SHALL return to
  `:healthy`

#### Scenario: A plugin restart resumes after the last committed line

- **GIVEN** a reader that committed the offset after line 2 of a 3-line object
- **WHEN** the plugin restarts
- **THEN** only line 3 SHALL be injected

#### Scenario: A forbidden request is a failure, not a missing object

- **WHEN** a request for the object fails with status `403`
- **THEN** health SHALL report a failure naming `403`, not `file_missing`, and the offset SHALL
  be unchanged

### Requirement: A file output plugin under an S3 root SHALL commit each roll as one object and guard the sidecar

Under an S3 root the output plugin SHALL accept only `commit: rename`, and `path` SHALL name the key
prefix that stands for the directory. A `roll_bytes` above 5 GiB SHALL be refused at attach
naming `roll_bytes`. The open roll's records SHALL be held in memory with their holds open. A
roll SHALL be committed by one PUT of `<path>/part-<N>`, with `N` zero-padded to 8 digits, then
the sidecar `<path>/.progress` SHALL be written by a conditional put carrying `N` that is refused
when the stored part number is higher, and the roll's holds SHALL be confirmed only after both
writes succeed.

On start the output plugin SHALL list the keys under `<path>/`, delete a part numbered one above the
sidecar, and refuse the start naming both numbers when a part is two or more above it. A worker
whose sidecar write is refused because the stored number is higher SHALL NOT confirm the roll's
holds, and SHALL stop reporting the stored number.

#### Scenario: A roll becomes one object

- **GIVEN** an output plugin under `s3://events/flezha` with `path: "out"` whose open roll holds `a` and `b`
- **WHEN** the roll closes as part 1
- **THEN** the object `out/part-00000001` SHALL hold `a\nb\n`, `out/.progress` SHALL name part 1,
  and the holds SHALL be confirmed only after both writes

#### Scenario: A crash between the part and the sidecar writes once

- **GIVEN** `out/part-00000004` was written and `out/.progress` still names part 3
- **WHEN** the output plugin starts
- **THEN** `out/part-00000004` SHALL be deleted, and the re-driven records SHALL be written once

#### Scenario: A part two ahead refuses the start

- **GIVEN** `out/.progress` names part 3 and `out/part-00000005` exists
- **WHEN** the output plugin starts
- **THEN** the start SHALL be refused naming parts 5 and 3, and nothing SHALL be deleted

#### Scenario: A worker behind a newer sidecar stops

- **GIVEN** `out/.progress` names part 7, written by another worker
- **WHEN** this worker commits its roll as part 5
- **THEN** the sidecar write SHALL be refused, the roll's holds SHALL NOT be confirmed, and the
  worker SHALL stop reporting part 7

#### Scenario: An oversized roll is refused

- **GIVEN** the root is `s3://events/flezha`
- **WHEN** an output plugin is attached with `commit: "rename"` and a `roll_bytes` of 6 GiB
- **THEN** the attach SHALL fail naming `roll_bytes`

