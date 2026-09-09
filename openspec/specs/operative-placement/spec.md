# operative-placement Specification

## Purpose
How the controller selects one runner from the candidate set for an operative:
the rendezvous ranking, the fairness cap and its knob, the fallback when every
candidate is at cap, the scope boundary against node selection, and the
observability of the resulting spread.
## Requirements
### Requirement: One runner is selected from the candidate set by bounded-load rendezvous hashing

The controller SHALL select an operative's runner from the candidate set by
rendezvous ranking narrowed by a fairness cap. The candidate set SHALL be
obtained from the single placement filter that `runner-factory-sync` requires,
and this selection SHALL NOT construct a candidate set of its own. Candidates
SHALL be ranked by a rendezvous hash of the operative id against the runner id,
and the selected runner SHALL be the highest-ranked candidate whose count of
bound operatives is below the cap. When every candidate is at or above the cap,
the highest-ranked candidate SHALL be selected, so a cap never refuses a
placement the filter allowed.

The count is a fairness input and not a measure of work. Two operatives are not
equal load, so an even count SHALL NOT be read as an even machine, and no
throughput guarantee is made here.

#### Scenario: A namespace with fewer operatives than runners leaves no runner idle

- **WHEN** eight active runners advertise factory `F` at the established language and eight operatives of factory `F` are started
- **THEN** each of the eight runners SHALL be bound exactly one of them

#### Scenario: Spread holds when operatives outnumber runners

- **WHEN** five active runners advertise factory `F` and twenty-five operatives of factory `F` are started
- **THEN** each runner SHALL be bound exactly five of them

#### Scenario: Every candidate at cap still places

- **WHEN** a placement is resolved and every candidate's bound count is at or above the cap
- **THEN** the highest-ranked candidate SHALL be selected and the placement SHALL succeed

#### Scenario: The filter still decides eligibility

- **WHEN** a runner advertising factory `F` at a language other than the established one holds no operatives at all
- **THEN** it SHALL NOT be selected for `F`, whatever its count

### Requirement: The fairness cap is operator-configurable and spans the whole range

The fairness cap SHALL be derived from a declared configuration knob expressed
as an integer percentage of the fair share, defaulting to `100`. The cap for a
placement SHALL be `max(1, ceil(percent * (bound + 1) / (100 * candidates)))`,
where `bound` is the number of operatives already counted against the candidate
set and `candidates` is its size. A percentage large enough that the cap always
exceeds every candidate's count SHALL reproduce selection by rendezvous hash
alone, so an operator can return to unweighted placement without a code change.

#### Scenario: The default is exactly the fair share

- **WHEN** the knob is left unset and operatives are started across a fixed runner set
- **THEN** the difference between the busiest and idlest runner's bound count SHALL be at most one

#### Scenario: A high percentage restores plain rendezvous selection

- **WHEN** the knob is set high enough that no candidate ever reaches the cap
- **THEN** every operative SHALL be placed on the candidate its rendezvous rank puts first

### Requirement: Node selection is not load-aware

Selection of a controller node SHALL use rendezvous hashing with no load input.
The re-placement path that chooses a destination node for an operative whose
local fleet cannot host its factory SHALL rank nodes by rendezvous hash alone,
so that repeated convergence ticks and concurrent loops on different nodes agree
on one destination. Runner counts SHALL NOT be consulted for a node decision.

#### Scenario: Two ticks agree on the destination node

- **WHEN** the same operative's re-placement is planned twice against an unchanged set of factory-advertising nodes
- **THEN** both SHALL name the same destination node, whatever the runner counts on either node

### Requirement: A count never refuses a placement or moves a healthy operative

A stale or inaccurate count SHALL NOT cause a placement to fail. Whenever the
filter returns a non-empty candidate set, selection SHALL return one of its
members. An operative already bound to a runner that is active and in the
candidate set for its factory SHALL NOT be re-placed on account of any count, so
that a load reading cannot move work that is running correctly.

#### Scenario: An inflated count still places

- **WHEN** every candidate's recorded count is higher than the operatives actually bound to it
- **THEN** the placement SHALL still return a candidate from the filter's set

#### Scenario: A healthy binding survives a convergence tick

- **WHEN** convergence evaluates an operative whose bound runner is active and advertises its factory at the established language
- **THEN** the operative SHALL remain on that runner and no rebind SHALL be issued

### Requirement: The spread is observable

The controller SHALL expose the number of operatives bound to each runner as a
declared measurement, through the same declaration mechanism every other
controller counter uses. The measurement SHALL carry the runner identity, so an
operator can see the distribution the cap governs rather than infer it.

#### Scenario: An operator reads the distribution

- **WHEN** operatives are bound across several runners in a namespace and the controller's measurements are scraped
- **THEN** the exposition SHALL report a per-runner bound-operative count for each of them
