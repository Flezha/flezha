package com.flezha.sdk.testing;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.RoutedDocument;
import com.flezha.sdk.runtime.operative.builder.LatestDocumentRouter;
import com.flezha.sdk.runtime.operative.builder.LatestDocumentRouter.Input;
import com.flezha.sdk.runtime.operative.builder.LatestDocumentRouter.RouterChange;
import com.flezha.sdk.runtime.operative.builder.LatestDocumentRouter.RouterState;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The delivery contract of the procedure {@code ChannelBuilder.routeLatest}
 * expands to — the Java mirror of the Rust SDK's {@code route_latest} cases.
 *
 * <p>The documents here are opaque {@code String}s, which is both what the block
 * promises (the document is retained and compared, never parsed) and what keeps
 * the harness's own state codec — built from {@code Procedure.kryoTypes()} alone,
 * with no wiring to register a document class — able to serialize the retained
 * state.
 */
final class RouteLatestRouterTest {

    private static ProcedureTestHarness<Input, RouterState, RoutedDocument<Object>, RouterChange> harness() {
        return ProcedureTestHarness.of(new LatestDocumentRouter());
    }

    private static Input document(String body) {
        return Input.ofDocument(body);
    }

    private static Input traffic(String key) {
        return Input.ofTrafficKey(key);
    }

    private static RoutedDocument<Object> delivery(String key, String body) {
        return new RoutedDocument<>(key, body);
    }

    @SuppressWarnings("unchecked")
    private static Class<RoutedDocument<Object>> routed() {
        return (Class<RoutedDocument<Object>>) (Class<?>) RoutedDocument.class;
    }

    /**
     * fails when: a changed document stops reaching a key on that key's own next
     * traffic item — every per-key consumer of an operative-wide document would
     * keep computing against a stale one, silently.
     */
    @Test
    void aChangedDocumentReachesEachKeyOnItsOwnNextTraffic() {
        harness()
                .applyInputs(List.of(
                        document("limit=10"),
                        traffic("AAPL"),
                        traffic("MSFT"),
                        // A second version: both keys are owed a delivery again,
                        // each on its own next item.
                        document("limit=20"),
                        traffic("MSFT"),
                        traffic("AAPL")))
                .expectEmits(
                        routed(),
                        List.of(
                                delivery("AAPL", "limit=10"),
                                delivery("MSFT", "limit=10"),
                                delivery("MSFT", "limit=20"),
                                delivery("AAPL", "limit=20")));
    }

    /**
     * fails when: a re-delivered identical document bumps the version — a
     * document source that re-polls after every reconnect would then re-deliver
     * to every key on every reconnect.
     */
    @Test
    void aRepeatedIdenticalDocumentIsInert() {
        harness()
                .applyInputs(List.of(
                        document("limit=10"),
                        traffic("AAPL"),
                        document("limit=10"),
                        traffic("AAPL"),
                        document("limit=10"),
                        traffic("AAPL")))
                .expectEmits(routed(), List.of(delivery("AAPL", "limit=10")));
    }

    /**
     * fails when: a key first seen after the document was retained never receives
     * it (a late-born Entity starts up config-less) or receives it repeatedly.
     */
    @Test
    void aFirstSeenKeyReceivesTheCurrentDocumentOnce() {
        harness()
                .applyInputs(List.of(
                        document("limit=10"), traffic("AAPL"), traffic("NVDA"), traffic("NVDA"), traffic("NVDA")))
                .expectEmits(routed(), List.of(delivery("AAPL", "limit=10"), delivery("NVDA", "limit=10")));
    }

    /**
     * fails when: a delivery is made for a key that produced no traffic — a
     * remembered-set fan-out. After a rescale such a key is no longer this
     * member's, and every delivery to it parks against the partition guard.
     */
    @Test
    void onlyKeysProvenByTrafficAreEmitted() {
        harness()
                .applyInputs(List.of(
                        document("limit=10"),
                        traffic("AAPL"),
                        traffic("MSFT"),
                        // Both keys are owed version 2, but only AAPL trades again.
                        document("limit=20"),
                        traffic("AAPL")))
                .expectEmits(
                        routed(),
                        List.of(
                                delivery("AAPL", "limit=10"),
                                delivery("MSFT", "limit=10"),
                                delivery("AAPL", "limit=20")));
    }

    /**
     * fails when: traffic arriving before any document produces a delivery —
     * there is nothing to deliver.
     */
    @Test
    void trafficBeforeTheFirstDocumentDeliversNothing() {
        harness().applyInputs(List.of(traffic("AAPL"), traffic("MSFT"))).expectEmits(routed(), List.of());
    }

    /**
     * fails when: the router's state or its emit stream stops being a pure
     * function of its inputs — a crash-resume or a rebind would then rebuild a
     * router that owes a different set of deliveries than the live run did.
     */
    @Test
    void routerStateReplaysDeterministically() {
        List<Input> inputs = List.of(
                document("limit=10"),
                traffic("AAPL"),
                traffic("MSFT"),
                document("limit=10"),
                traffic("AAPL"),
                document("limit=20"),
                traffic("AAPL"),
                traffic("NVDA"));
        var h = harness().applyInputs(inputs);
        assertThat(h.replayEquals()).isEmpty();
        assertThat(h.replayEmitsEqual(inputs)).isEmpty();
    }
}
