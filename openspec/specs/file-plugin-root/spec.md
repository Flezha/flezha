# file-plugin-root Specification

## Purpose
TBD - created by archiving change add-a-file-plugin. Update Purpose after archive.
## Requirements
### Requirement: Every file plugin path SHALL resolve inside one configured root folder

The controller SHALL declare one configuration row, `FLEZHA_FILE_PLUGIN_ROOT`, naming the one
folder, or the one `s3://` bucket prefix, under which file plugins read and write. An `s3://`
root SHALL follow `file-plugin-s3`, and the folder rules of this requirement SHALL NOT apply to
it. Its default SHALL be the folder `storage/app/files`, which the first file plugin enable SHALL
create when it is missing. A folder the user sets SHALL NOT be created, and while it is
missing every file plugin enable SHALL be refused naming it.

Each namespace SHALL have its own folder, `<root>/<namespace>`, and a plugin SHALL read and write
only inside the folder of the namespace it is enabled in. The namespace folder SHALL be created,
readable by the controller's user only, on the first enable that needs it, when the root exists.

Under a folder root, a plugin's `path` SHALL be expanded against its namespace folder, so a
relative path names a file under it and an absolute path is taken as written, and SHALL then be
resolved with symlinks followed, component by component under a bounded number of hops. The
resolved path SHALL start with the resolved namespace folder, or the enable SHALL be refused
naming that folder. The file itself need not exist at enable time, but its parent directory SHALL
exist and SHALL resolve inside the namespace folder.

The resolved absolute path SHALL replace `path` in the plugin's options, so every log line,
telemetry event and health verdict names one spelling.

#### Scenario: The root is not set

- **WHEN** a file plugin is enabled in namespace `orders` and `FLEZHA_FILE_PLUGIN_ROOT` is not set
- **THEN** the root SHALL be `storage/app/files`, created if it is missing, the folder
  `storage/app/files/orders` SHALL be created, and the enable SHALL succeed

#### Scenario: A configured root that is missing is refused

- **GIVEN** the root is set to `/srv/flezha/files`, which does not exist
- **WHEN** a file plugin is enabled
- **THEN** the enable SHALL be refused naming `/srv/flezha/files`, and the folder SHALL NOT be
  created

#### Scenario: An S3 root is not treated as a folder

- **GIVEN** the root is set to `s3://events/flezha`
- **WHEN** a file plugin is enabled with `path: "echo"`
- **THEN** no folder SHALL be looked for or created, and the path SHALL be resolved by
  `file-plugin-s3`

#### Scenario: A relative path lands under the root

- **GIVEN** the root is `/srv/flezha/files`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "events.ndjson"`
- **THEN** the enable SHALL succeed and the plugin's resolved path SHALL be
  `/srv/flezha/files/orders/events.ndjson`

#### Scenario: A path that climbs out is refused

- **GIVEN** the root is `/srv/flezha/files`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "../certs/controller.key"`
- **THEN** the enable SHALL be refused naming `/srv/flezha/files/orders`

#### Scenario: An absolute path outside the root is refused

- **GIVEN** the root is `/srv/flezha/files`
- **WHEN** a plugin is enabled with `path: "/etc/passwd"`
- **THEN** the enable SHALL be refused naming the namespace folder under `/srv/flezha/files`

#### Scenario: A symlink pointing out of the root is refused

- **GIVEN** `/srv/flezha/files/orders/out` is a symlink to `/etc`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "out/hosts"`
- **THEN** the enable SHALL be refused naming the namespace folder, because the resolved path is
  outside it

#### Scenario: A symlink pointing inside the root is allowed

- **GIVEN** `/srv/flezha/files/orders/current` is a symlink to `/srv/flezha/files/orders/2026-09`
- **WHEN** a plugin is enabled in namespace `orders` with `path: "current/events.ndjson"`
- **THEN** the enable SHALL succeed and the resolved path SHALL be
  `/srv/flezha/files/orders/2026-09/events.ndjson`

#### Scenario: A missing parent is refused

- **WHEN** a plugin is enabled with a path whose parent directory does not exist
- **THEN** the enable SHALL be refused naming the missing directory

#### Scenario: Another namespace's folder is refused

- **GIVEN** the root is `/srv/flezha/files`
- **WHEN** a plugin is enabled in namespace `payments` with `path: "../orders/in/trades.jsonl"`
  or with `path: "/srv/flezha/files/orders/in/trades.jsonl"`
- **THEN** the enable SHALL be refused naming `/srv/flezha/files/payments`

#### Scenario: A symlink into another namespace's folder is refused

- **GIVEN** `/srv/flezha/files/payments/peek` is a symlink to `/srv/flezha/files/orders`
- **WHEN** a plugin is enabled in namespace `payments` with `path: "peek/events.ndjson"`
- **THEN** the enable SHALL be refused naming `/srv/flezha/files/payments`

### Requirement: A file the output plugin creates SHALL be private and SHALL NOT be created through a symlink

A file the output plugin creates SHALL have mode `0600`. A file that already exists SHALL keep the mode
it has. If the path names a symlink at open time, the open SHALL be refused rather than
followed, even when the link resolves inside the root.

#### Scenario: A created file is private

- **WHEN** the output plugin opens a path that does not exist
- **THEN** the file SHALL exist afterwards with mode `0600`

#### Scenario: An existing file keeps its mode

- **GIVEN** a file with mode `0644` at the output plugin's path
- **WHEN** the output plugin opens it and appends
- **THEN** its mode SHALL still be `0644`

#### Scenario: A symlink is not opened

- **GIVEN** the output plugin's path is a symlink to a file inside the root
- **WHEN** the output plugin attempts to open it
- **THEN** the open SHALL be refused and the plugin SHALL report unhealthy naming the path

### Requirement: A file plugin SHALL NOT log file contents

A log line, telemetry event or health verdict SHALL name the path, an offset or a byte count.
It SHALL NOT carry the bytes of any line read or written.

#### Scenario: A refused line names its offset

- **WHEN** the reader refuses a line at byte offset 4096
- **THEN** the warning and the health verdict SHALL name the path and `4096`, and SHALL NOT
  contain the line's bytes

### Requirement: A restored file plugin outside its namespace folder SHALL be refused naming the new location

A restored file plugin binding SHALL be refused when its stored path lies outside its namespace
folder, after a restart or an upgrade alike, and the refusal SHALL name both the stored path and
the path it would have under the namespace folder. Nothing SHALL be moved,
read or written for it.

#### Scenario: A binding stored under the shared folder is refused on restore

- **GIVEN** a file sink in namespace `orders` was stored with path `/srv/flezha/files/out.ndjson`
- **WHEN** the controller restores it
- **THEN** the binding SHALL be refused naming `/srv/flezha/files/out.ndjson` and
  `/srv/flezha/files/orders/out.ndjson`, and no file SHALL be created or opened

