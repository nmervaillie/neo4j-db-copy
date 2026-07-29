package org.neo4j.dbcopy;

import java.util.Locale;

public class ProgressBar {

    private final String itemType;
    private final long total;
    private final long startTimeNanos;
    private int processed;
    private int lastPrintedProgress = -1;

    public ProgressBar(String itemType, long total) {
        this.itemType = itemType;
        this.total = total;
        this.startTimeNanos = System.nanoTime();
        this.processed = 0;
    }

    public void updateProgress(int batchSize) {
        processed += batchSize;
        int progress = total == 0 ? 100 : (int) (processed * 100L / total);
        if (progress == lastPrintedProgress) {
            return;
        }
        lastPrintedProgress = progress;
        double elapsedSeconds = Math.max((System.nanoTime() - startTimeNanos) / 1_000_000_000.0, 0.001d);
        double averagePerSecond = processed / elapsedSeconds;
        StringBuilder bar = new StringBuilder("[");

        for (int i = 0; i < 50; i++) {
            if (i < progress / 2) {
                bar.append("=");
            } else {
                bar.append(" ");
            }
        }

        bar.append("] ")
                .append(progress)
                .append("% ")
                .append(itemType)
                .append(" copied (")
                .append(processed)
                .append("/")
                .append(total)
                .append(")")
                .append(" avg ")
                .append(String.format(Locale.ROOT, "%.0f", averagePerSecond))
                .append("/s");
        System.out.print("\r" + bar);

        if (progress >= 100) {
            System.out.println();
        }
    }
}
