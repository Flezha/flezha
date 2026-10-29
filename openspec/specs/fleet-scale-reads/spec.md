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

### Requirement: A list read SHALL answer one page, its total and a cursor when asked

The relay list, the enabled-plugin list, the dead-letter list and the steps reads (journal, committed iterations, Cells, undelivered) SHALL take an optional `opts` keyword of `limit`, `after`, `sort` and `filter`. Without `opts` each SHALL answer exactly what it answered before.

With `opts` it SHALL answer:

- at most `limit` rows matching the filter, in the declared sort order;
- `total`, every matching row before the limit;
- `next`, a cursor for the following page, or `nil` on the last page;
- `unreachable`.

`limit` SHALL default to 50 and SHALL NOT exceed 500, and `0` SHALL answer only the totals. An unknown key, a limit outside its bounds, and a cursor the controller did not issue for that read and sort SHALL be refused as `invalid` before any work. No operation SHALL be added for paging: each collection keeps its one operation.

#### Scenario: Paging through a namespace

- **GIVEN** a namespace running 1,200 relays
- **WHEN** a caller reads the relay list with `limit: 500`, passing each answer's `next` as `after`
- **THEN** three reads SHALL answer 500, 500 and 200 rows, each SHALL carry `total: 1200`, and the third SHALL carry `next: nil`

#### Scenario: A relay stops between two pages

- **GIVEN** a caller has read the first page sorted by id
- **WHEN** a relay on that page stops and the caller reads the next page with the cursor
- **THEN** the next page SHALL start at the first relay after the last one the caller received, repeating none and skipping none of the relays still running

#### Scenario: A forged cursor

- **WHEN** a caller passes a cursor the controller did not issue, or one issued for another read or sort
- **THEN** the read SHALL be refused as `invalid` and SHALL answer no rows

#### Scenario: Existing callers are unchanged

- **WHEN** `flz relay list` or an SDK's `list_relays()` calls the relay list without `opts`
- **THEN** the answer SHALL be the one it received before this requirement

### Requirement: The relay list SHALL filter, sort and summarise a namespace in one read

A paged relay list SHALL filter by health, factory, runner, node, partition group, relay ids (at most 500) and a case-insensitive text search over relay id and factory id. It SHALL sort by id or by severity. A partition group SHALL count as one row, and each row SHALL carry its run kind and bound-plugin count.

Each paged answer SHALL carry the namespace's `summary`, computed over every relay whatever the filter: counts by health, the faulted partition groups, and the factories relays wait on with how many relays wait on each. When the filter names only `ids`, the read SHALL look those relays up directly, its cost SHALL NOT grow with the namespace's other relays, and its `summary` SHALL be `nil`.

#### Scenario: Finding one relay among thousands

- **GIVEN** a namespace running 5,000 relays
- **WHEN** a caller reads the relay list with `filter: [text: "00500"]`
- **THEN** the rows SHALL be the relays whose id or factory contains `00500`, and `total` SHALL count them

#### Scenario: A status line without rows

- **GIVEN** a namespace running 3,000 relays, 12 of them failed
- **WHEN** a caller reads the relay list with `limit: 0`
- **THEN** the answer SHALL hold no rows, and its summary's health counts SHALL sum to 3,000 with 12 failed

#### Scenario: A runner goes away

- **GIVEN** 1,000 relays of factory `orders` whose only runner disconnected
- **WHEN** a caller reads the relay list with `filter: [health: unhealthy]`, `sort: :severity` and `limit: 20`
- **THEN** the answer SHALL hold 20 rows with `total: 1000`, and the summary's `waiting` SHALL name `orders` with 1,000 relays

#### Scenario: One relay screen in a large namespace

- **GIVEN** a namespace running 5,000 relays
- **WHEN** a caller reads the relay list with `filter: [ids: ["orders_17"]]`
- **THEN** the one row SHALL equal that relay's row in the unpaged list, and the read SHALL not walk the namespace

### Requirement: A paged plugin list SHALL carry a sample of each plugin's relays

A paged enabled-plugin list SHALL filter by provider, by a bound relay and by a name search. Each row SHALL carry at most 10 bound relay ids, sorted, and `relay_count`. The unpaged list SHALL keep every bound relay.

#### Scenario: A plugin bound to a thousand relays

- **GIVEN** a plugin bound to 1,000 relays
- **WHEN** a caller reads the page holding it
- **THEN** its row SHALL carry 10 relay ids and `relay_count: 1000`

#### Scenario: The plugins of one relay

- **WHEN** a caller reads the plugin list with `filter: [relay: "orders_17"]`
- **THEN** the rows SHALL be exactly the plugins bound to `orders_17`

### Requirement: Liveness counters SHALL be readable for named relays

The liveness counters SHALL key every relay by its namespace and id, and every relay row SHALL carry its `namespace`. Two relays with the same id in different namespaces SHALL keep separate counters.

The liveness counters SHALL take an optional `opts` of `filter` with either `relays: [{namespace, relay_id}]` (at most 500 relays) or `namespace: ns`, and `limit`. Any other key, and `relays` together with `namespace`, SHALL be refused as `invalid`. They SHALL then answer, across the cluster and in the same `channels`, `relays` and `unreachable` shape:

- for `relays`: only those relays' counters, matched on namespace and id, and the channel counters of the runners those relays are bound to;
- for `namespace`: every relay of that namespace and the channel counters of the runners they are bound to;
- for `namespace` with `limit: 0`: no rows, and `totals`, each counted relay slot summed over the namespace's relays on every controller.

#### Scenario: Rates for one page

- **GIVEN** a cluster running 5,000 relays
- **WHEN** a caller asks for the counters of the 20 relays on its page
- **THEN** the answer SHALL hold those 20 relays' counters and their runners' channel counters, and no other relay's

#### Scenario: The same id in two namespaces

- **GIVEN** a relay `orders` in namespace `a` and another `orders` in namespace `b` on one controller, each having received a different number of records
- **WHEN** a caller reads the liveness counters
- **THEN** the answer SHALL hold two rows for `orders`, one per namespace, each with its own counts

#### Scenario: A namespace's flow without its rows

- **GIVEN** a namespace running 5,000 relays across two controllers
- **WHEN** a caller reads the liveness counters with `filter: [namespace: "trade"]` and `limit: 0`
- **THEN** the answer SHALL hold no relay or channel rows
- **AND** its `totals` SHALL equal, slot by slot, the sum of the counted slots of that namespace's relay rows read without `limit: 0` at the same moment

#### Scenario: One scope per read

- **WHEN** a caller passes both `relays` and `namespace`
- **THEN** the read SHALL be refused as `invalid`

### Requirement: The undelivered input-log records SHALL be countable without reading them

The undelivered read with `opts` SHALL answer `total` from the input log's offsets, without reading a record. It SHALL read only the records of the page it answers. Its `total` SHALL equal the length of the unpaged answer read at the same moment.

#### Scenario: A large backlog

- **GIVEN** an instance with 50,000 undelivered records
- **WHEN** a caller reads undelivered with `limit: 0`
- **THEN** the answer SHALL carry `total: 50000` and no rows, and no record SHALL be read from disk

### Requirement: A steps instance's journal, iterations and Cells SHALL be readable by the page

Journal and committed iterations SHALL page newest first. Cells SHALL page by key, with a key search.

#### Scenario: Picking a rewind point in a long history

- **GIVEN** an instance with 20,000 committed iterations
- **WHEN** a caller reads committed iterations with `limit: 50`
- **THEN** the answer SHALL hold the 50 newest, `total: 20000` and a cursor

### Requirement: Dead-letter statistics SHALL be read without decoding entries

The dead-letter Store SHALL keep entry and byte counts per relay class and tasklet in memory, updated after an entry is parked, dropped, resolved or swept and after a relay's parked sets are hydrated, and rebuilt from disk at start. Stats SHALL answer from those counts without reading an entry index. A paged dead-letter list SHALL answer newest first across the cluster and SHALL decode only the entries of the page it answers.

#### Scenario: Stats with a large queue

- **GIVEN** 100,000 parked entries across 40 tasklets
- **WHEN** a caller reads dead-letter stats
- **THEN** the answer SHALL give each tasklet's entry and byte counts, and no entry index SHALL be read

#### Scenario: Counts survive a crash

- **GIVEN** the Store is killed during a sweep
- **WHEN** it restarts and a caller reads stats
- **THEN** the counts SHALL equal the entries on disk

#### Scenario: A drop between two pages

- **GIVEN** a caller has read the first page of the dead-letter list
- **WHEN** an entry on that page is dropped and the caller reads the next page with the cursor
- **THEN** the next page SHALL repeat none and skip none of the entries still parked

