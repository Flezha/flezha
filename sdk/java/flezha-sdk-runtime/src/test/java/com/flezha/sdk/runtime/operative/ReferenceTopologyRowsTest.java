package com.flezha.sdk.runtime.operative;

import static com.flezha.sdk.runtime.operative.StreamOperativeTopologyBuilderTest.catchConfigException;
import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.runtime.RepositoryFixture;
import com.flezha.sdk.runtime.core.DerivedDeclaration;
import com.flezha.sdk.runtime.core.HostClassDeclaration;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The rows of {@code test/fixtures/reference_topologies.txt}, authored through this SDK's builder.
 *
 * <p>That fixture is the cross-language contract: one row per refusal
 * {@code flz_core::host::register::check} names, and the channels a well-formed row derives to. The
 * whole table runs through the row interpreter in {@code test/apps/java_runner} under
 * {@code :interop}; these two cases keep the fixture inside the Java build itself, where a pull
 * request can see it.
 *
 * <p>The fixture states the verdict and this class states the authoring: each row is written as the
 * fluent calls its {@code steps} column spells, and the expected refusal name and the expected
 * derived lines are read out of the file, so a row it renames or drops fails here rather than
 * passing on a stale literal. {@code StreamOperativeTopologyBuilderTest.CoreRefusals} is the other
 * half, pinning the SENTENCES for the shapes this SDK used to judge in its own words; its procedures
 * and its verdict catcher are reused here rather than declared a second time.
 *
 * <p>What makes the refusal case a statement about the core rather than about a string:
 * {@code StreamOperativeConfigurationException.refusals()} carries the names of the verdict, and
 * nothing the builder still refuses itself can put a reference variant there. {@code TypeTokens}
 * throws with no names at all, and {@code StreamOperativeTopologyBuilder.refused} names every
 * sentence it aggregates {@code Declaration}, which the fixture gives no row, on purpose. Note
 * what that rules out: {@code refusals()} being EMPTY does not mean "the builder refused this",
 * and asserting so would fail on every refusal that goes through {@code refused}.
 */
class ReferenceTopologyRowsTest {

    private static final String FIXTURE = "test/fixtures/reference_topologies.txt";

    /** One authored row: the fixture's topology id, and what its {@code steps} column spells. */
    private record Row(String topology, Consumer<ChannelBuilder> authoring) {}

    /**
     * The malformed rows authored here, chosen from the reference names no {@code sdk/java} test
     * asserted. The fixture's palette roles bind to procedures the builder test already declares:
     * {@code plain} is {@code SingleProcedure}, {@code second} {@code GraphStage1}, {@code routed}
     * {@code StaticProcedure}, and {@code dynamic} {@code DynamicModeOnlyProcedure}, whose keyed
     * edge is what makes it dynamic.
     */
    private List<Row> malformed() {
        return List.of(
                // entry(in); in.key.key.into(plain)
                new Row("dup_key_by", c -> c.input(String.class, "in")
                        .keyBy(value -> value)
                        .keyBy(value -> value)
                        .into(StreamOperativeTopologyBuilderTest.SingleProcedure.class)),
                // entry(in); in.key.map.into(plain)
                new Row("map_after_key", c -> c.input(String.class, "in")
                        .keyBy(value -> value)
                        .map(String.class, value -> value)
                        .into(StreamOperativeTopologyBuilderTest.SingleProcedure.class)),
                // entry(in); in.into(plain); plain.sink(out); plain.map.into(second)
                new Row("map_after_sink", c -> {
                    var plain =
                            c.input(String.class, "in").into(StreamOperativeTopologyBuilderTest.SingleProcedure.class);
                    plain.sink("out");
                    plain.map(String.class, value -> value).into(StreamOperativeTopologyBuilderTest.GraphStage1.class);
                }),
                // entry(in); in.into(plain); in.map
                new Row("dangling_chain", c -> {
                    var in = c.input(String.class, "in");
                    in.into(StreamOperativeTopologyBuilderTest.SingleProcedure.class);
                    in.map(String.class, value -> value);
                }),
                // entry(in); in.map.into(plain); in.into(plain)
                new Row("edge_rewired", c -> {
                    var in = c.input(String.class, "in");
                    in.map(String.class, value -> value).into(StreamOperativeTopologyBuilderTest.SingleProcedure.class);
                    in.into(StreamOperativeTopologyBuilderTest.SingleProcedure.class);
                }),
                // entry(in); in.key.into(routed)
                new Row("key_on_static", c -> c.input(String.class, "in")
                        .keyBy(value -> value)
                        .into(StreamOperativeTopologyBuilderTest.StaticProcedure.class)),
                // entry(in); entry(other); in.key.into(dynamic); other.into(dynamic)
                new Row("unkeyed_dynamic", c -> {
                    var in = c.input(String.class, "in");
                    var other = c.input(String.class, "other");
                    in.keyBy(value -> value).into(StreamOperativeTopologyBuilderTest.DynamicModeOnlyProcedure.class);
                    other.into(StreamOperativeTopologyBuilderTest.DynamicModeOnlyProcedure.class);
                }),
                // entry(in); in.into(plain); plain.named(a); plain.sink(b)
                new Row("named_twice", c -> c.input(String.class, "in")
                        .into(StreamOperativeTopologyBuilderTest.SingleProcedure.class)
                        .named("a")
                        .sink("b")),
                // entry(in); in.into(plain); in.into(second); plain.sink(out); second.sink(out)
                new Row("two_channels_one_sink", c -> {
                    var in = c.input(String.class, "in");
                    in.into(StreamOperativeTopologyBuilderTest.SingleProcedure.class)
                            .sink("out");
                    in.into(StreamOperativeTopologyBuilderTest.GraphStage1.class)
                            .sink("out");
                }));
    }

    /** The well-formed rows whose derived lines the fixture's seven-column table carries. */
    private List<Row> wellFormed() {
        return List.of(
                // entry(in); in.into(plain); in.into(plain)
                new Row("fan_in_noop", c -> {
                    var in = c.input(String.class, "in");
                    in.into(StreamOperativeTopologyBuilderTest.SingleProcedure.class);
                    in.into(StreamOperativeTopologyBuilderTest.SingleProcedure.class);
                }),
                // entry(in); in.into(plain); plain.named(renamed); plain.sink
                new Row("named_sink", c -> {
                    var plain =
                            c.input(String.class, "in").into(StreamOperativeTopologyBuilderTest.SingleProcedure.class);
                    plain.named("renamed");
                    plain.sink();
                }),
                // entry(in); in.into(plain); plain.sink(items_output)
                new Row("sink_names_channel", c -> c.input(String.class, "in")
                        .into(StreamOperativeTopologyBuilderTest.SingleProcedure.class)
                        .sink("items_output")));
    }

    @Test
    @DisplayName("A malformed row refuses with the reference name")
    void aMalformedRowRefusesWithTheReferenceName() throws Exception {
        for (Row row : malformed()) {
            String name = refusalOf(row.topology());
            StreamOperativeConfigurationException refused =
                    catchConfigException(() -> StreamOperativeTopologyBuilder.build(row.topology(), row.authoring()));

            assertThat(refused)
                    .as("%s is a refusal row of %s and this builder accepted it", row.topology(), FIXTURE)
                    .isNotNull();
            assertThat(refused.refusals())
                    .as(
                            "the names the core listed in its verdict for %s. What the builder"
                                    + " still refuses itself names no reference variant: an"
                                    + " unresolved type token carries no names at all and the"
                                    + " rest are all `Declaration`, which the fixture gives no"
                                    + " row, so a reference name here can only be the core's",
                            row.topology())
                    .contains(name);
            assertThat(refused)
                    .as("the rendered verdict an author reads for %s", row.topology())
                    .hasMessageContaining(name);
        }
    }

    @Test
    @DisplayName("A well-formed row derives to the fixture's bytes")
    void aWellFormedRowDerivesToTheFixturesBytes() throws Exception {
        for (Row row : wellFormed()) {
            StreamOperativeTopology topology = StreamOperativeTopologyBuilder.build(row.topology(), row.authoring());
            // The record the builder submitted, answered by the same check it took its
            // verdict from: the derived declaration itself, not a second reading of it.
            DerivedDeclaration derived = HostClassDeclaration.check(topology.declaration());

            assertThat(columns(derived))
                    .as("%s derived a declaration %s does not pin", row.topology(), FIXTURE)
                    .isEqualTo(pinnedLines(row.topology()));
        }
    }

    /** The refusal the fixture's four-column table names for a row. */
    private String refusalOf(String topology) throws IOException {
        for (List<String> row : RepositoryFixture.rows(FIXTURE)) {
            if (row.size() == 4 && row.get(1).equals(topology)) {
                assertThat(row.get(0))
                        .as("%s is a well-formed row, so it refuses with nothing", topology)
                        .isNotEqualTo("ok");
                return row.get(0);
            }
        }
        throw new AssertionError(FIXTURE + " carries no row `" + topology + "`");
    }

    /** The six columns the fixture pins for one topology's derived declaration. */
    private List<List<String>> pinnedLines(String topology) throws IOException {
        List<List<String>> lines = new ArrayList<>();
        for (List<String> row : RepositoryFixture.rows(FIXTURE)) {
            if (row.size() == 7 && row.get(0).equals(topology)) {
                lines.add(row.subList(1, 7));
            }
        }
        if (lines.isEmpty()) {
            throw new AssertionError(FIXTURE + " pins no derived lines for `" + topology + "`");
        }
        return lines;
    }

    /** The core's answer in the fixture's columns: index, name, kind, producer, consumers, sink. */
    private List<List<String>> columns(DerivedDeclaration derived) {
        List<List<String>> lines = new ArrayList<>();
        for (DerivedDeclaration.Channel channel : derived.channels()) {
            lines.add(List.of(
                    String.valueOf(channel.index()),
                    channel.name(),
                    channel.kind(),
                    absent(channel.producer()),
                    channel.consumers().isEmpty()
                            ? "-"
                            : channel.consumers().stream()
                                    .map(edge -> String.valueOf(edge.procedure()))
                                    .collect(Collectors.joining(",")),
                    absent(channel.sink())));
        }
        return lines;
    }

    /** The fixture spells an absent column `-`. */
    private String absent(Object value) {
        return value == null ? "-" : String.valueOf(value);
    }
}
