package com.flezha.sdk.runtime.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.RepositoryFixture;
import org.junit.jupiter.api.Test;

/**
 * This runtime's reading of the shared parse fixture.
 *
 * <p>A knob's name, type and default are declared once in {@code Flezha.Config.Keys}; the fixture
 * {@code test/fixtures/knob_parse_cases.txt} is the parse rule that declaration promises, so a Java
 * runner answers {@code "0"} exactly as the controller and every other SDK do. Java's host-side
 * surface is counts alone, so the count rule is the one this runtime carries.
 */
class KnobParseRuleTest {

    private static final int SENTINEL = -1;

    @Test
    void everyCountCaseParsesTheWayTheFixtureSays() throws Exception {
        boolean drove = false;
        // The line is split here rather than by the reader: ` 7 ` is a case about
        // the stray space, and it is an interior column, so splitting here keeps
        // it. A column the reader trimmed would prove nothing about the trimming
        // FlezhaEnv does.
        for (String line : RepositoryFixture.lines("test/fixtures/knob_parse_cases.txt")) {
            String[] parts = line.split("\\|");
            if (!parts[0].equals("positive_int")) {
                continue;
            }
            drove = true;
            String raw = parts[1].equals("<empty>") ? "" : parts[1];
            int got = FlezhaEnv.parsePositive("FLZ_TEST_A_COUNT", raw, SENTINEL);
            String rendered = got == SENTINEL ? "error" : String.valueOf(got);
            assertThat(rendered).as("%s|%s", parts[0], raw).isEqualTo(parts[2]);
        }
        assertThat(drove).as("the fixture declares no count case").isTrue();
    }

    @Test
    void anUnsetVariableLeavesTheDefaultStanding() {
        assertThat(FlezhaEnv.parsePositive("FLZ_TEST_A_COUNT", null, 7)).isEqualTo(7);
    }
}
