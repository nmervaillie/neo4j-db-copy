package org.neo4j.dbcopy;

public class ProgressBar {

    private final String itemType;
    private final long total;
    private int processed;

    public ProgressBar(String itemType, long total) {
        this.itemType = itemType;
        this.total = total;
        this.processed = 0;
    }

    public void updateProgress(int batchSize) {
        processed += batchSize;
        int progress = (int) ((processed / (double) total) * 100);
        StringBuilder bar = new StringBuilder("[");

        for (int i = 0; i < 50; i++) {
            if (i < progress / 2) {
                bar.append("=");
            } else {
                bar.append(" ");
            }
        }

        bar.append("] ").append(progress).append("% ").append(itemType).append(" copied (").append(processed).append("/").append(total).append(")");
        System.out.print("\r" + bar);

        if (progress >= 100) {
            System.out.println();
        }
    }
}
