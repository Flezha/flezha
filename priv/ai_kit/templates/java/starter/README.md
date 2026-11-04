# Starter: a relay that counts visits per page

A small, complete Flezha project in Java, to grow into your own: one relay, one
tasklet that keeps state, and a test. `pom.xml` names the SDK by its released
version, the way your own project does.

| File | What it is |
|---|---|
| `CountHits.java` | the tasklet `count`: keeps a page's total in state, changes it only through the `Counted` change, and answers the new count |
| `StarterRelay.java` | the relay `starter`: the entry channel `hit`, described and keyed by page, into `count`, out on `counts` |
| `Main.java` | the runner: serves the relay until shutdown |
| `CountHitsTest.java` | the test: four hits with no controller, and the replay check |

## Run it

You need a controller running and reachable, as
[Start the controller](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md#start-the-controller)
starts one. From this folder, build the jar (the test runs first) and start the runner:

```sh
mvn -q package
java -jar target/starter.jar
```

Then, from another terminal, start the relay and call it:

```sh
flz relay start starter --id starter
flz relay call starter --channel hit --arg page=home
```

```
{"page":"home","total":1}
```

Each call for the same page answers the next total. `flz` is the release's
`bin/flz`. The namespace is the runner's first argument and defaults to
`default`.

## Make it yours

Nothing inside the files is renamed for you. Change the package
`com.flezha.examples.starter`, and the `groupId` and `artifactId` in `pom.xml`.
`starter` is the name `flz relay start` takes. `count` is the name the
tasklet's state is saved under, so choose it before the first run.

## Building from a checkout

To run the project against a Flezha checkout instead of the published SDK, install
the checkout's `com.flezha:flezha-sdk:0.1.1` into your local Maven repository, which
Maven reads before Maven Central. From the root of the checkout:

```sh
scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
mvn -f sdk/java/pom.xml -DskipTests install
```
