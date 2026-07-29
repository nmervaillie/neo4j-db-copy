package org.neo4j.dbcopy;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressBarTest {

    @Test
    void should_only_redraw_when_percentage_changes() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(captured));
        try {
            ProgressBar progressBar = new ProgressBar("Nodes", 10000);
            for (int i = 0; i < 100; i++) {
                progressBar.updateProgress(1);
            }
        } finally {
            System.setOut(originalOut);
        }

        long redraws = captured.toString().chars().filter(c -> c == '\r').count();

        // 100 updates of 1/10000 each: percentage stays at 0% until the 100th update reaches 1%,
        // so only 2 redraws are expected (0% then 1%) instead of 100.
        assertThat(redraws).isEqualTo(2);
    }

    @Test
    void should_always_redraw_when_percentage_changes_every_time() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(captured));
        try {
            ProgressBar progressBar = new ProgressBar("Nodes", 100);
            for (int i = 0; i < 100; i++) {
                progressBar.updateProgress(1);
            }
        } finally {
            System.setOut(originalOut);
        }

        long redraws = captured.toString().chars().filter(c -> c == '\r').count();

        assertThat(redraws).isEqualTo(100);
    }
}
