package com.flezha.examples;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.examples.pipeline.PipelineOperative;
import com.flezha.sdk.LocalOperative;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PipelineTest {

    @Test
    @DisplayName("what the first procedure emits becomes the second one's input")
    void twoStagesRunInOrder() throws Exception {
        List<String> out = new ArrayList<>();

        try (LocalOperative run = LocalOperative.start(PipelineOperative.class)) {
            run.onSink("shouted", frame -> out.add(new String(frame.payload(), UTF_8)));
            run.feed("in", "  Hello World  ");
            run.runUntilIdle(Duration.ofSeconds(5));
        }

        assertThat(out).containsExactly("HELLO WORLD!");
    }
}
