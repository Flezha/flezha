# fleet-scale-reads Specification

## Purpose
How the controller's operator reads behave as a fleet grows to thousands of relays and plugins: reading one subject costs the same whatever the fleet size, and a listing costs time linear in the rows it answers.

## Requirements
### Requirement: Reading one subject's health SHALL NOT grow with the fleet

Reading the convergence verdict or the listed health of one subject (a relay, runner, plugin, channel or driver) SHALL copy only that subject's verdict and the small per-tick facts. Its cost SHALL NOT grow with the number of subjects the controller judges.

#### Scenario: One lookup in a large fleet

- **GIVEN** a controller that judges 8,000 subjects
- **WHEN** a caller reads one relay's health
- **THEN** the read SHALL take about the same time as with 1,000 subjects judged, within a factor of 2

#### Scenario: The answer is unchanged

- **GIVEN** a relay whose verdict is `:failed`, inside the unmet-intent grace
- **WHEN** a caller reads its listed health
- **THEN** the answer SHALL be `:recovering`, as before this requirement

### Requirement: A namespace's relay listing SHALL cost time linear in its relays

A read that lists a namespace's relays with their health (relay rows, the relay list, the relay info report, cluster status) SHALL read every relay's health in one pass over the namespace's verdicts. It SHALL NOT read the verdicts once per row. Its cost SHALL grow linearly with the namespace's relays.

#### Scenario: A thousand relays

- **GIVEN** a namespace running 1,000 relays on one controller
- **WHEN** a caller reads its relay rows
- **THEN** the answer SHALL arrive in well under the caller's default timeout, and each row's health SHALL equal what a single-relay read answers for that relay

#### Scenario: A relay not yet judged

- **GIVEN** a registry row for a relay the Loop has not judged yet
- **WHEN** a caller reads the relay rows
- **THEN** that row's health SHALL be `:unknown`

### Requirement: A namespace's plugin listing SHALL cost time linear in its plugins and bindings

The enabled-plugin listing SHALL group the namespace's bindings and running plugins by plugin once. It SHALL build each plugin's row from that plugin's group, without scanning every binding or every relay once per plugin.

#### Scenario: Plugins that grow with relays

- **GIVEN** a namespace where each of 2,000 relays has its own bound plugin
- **WHEN** a caller lists the enabled plugins
- **THEN** each row SHALL name exactly the relays bound to that plugin and the controllers it runs on
- **AND** the listing SHALL take no more than about 4 times as long as for 500 such relays and plugins

### Requirement: Every whole-fleet health reader SHALL read every verdict

A reader that folds over every verdict the controller judges, such as the health endpoint, SHALL read all of them in one read. It SHALL count the same subjects the Loop published on its last tick.

#### Scenario: The health endpoint counts every relay

- **GIVEN** the Loop has published verdicts for 3 relays, one of them failed
- **WHEN** the health endpoint is read
- **THEN** its relay health SHALL count 3 relays, one of them failed

