package org.neo4j.dbcopy.parquet;

import dev.hardwood.OutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class HardwoodSmokeTest {

    @Test
    void should_write_and_read_a_parquet_file(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("smoke.parquet");
        FileSchema schema = FileSchema.builder("smoke")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                        new LogicalType.StringType())
                .build();

        WriterConfig config = WriterConfig.builder().codec(CompressionCodec.ZSTD).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema, config)) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setLong("id", 1L).setString("name", "Ada"));
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rows = reader.rowReader()) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(1L);
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(1L);
            assertThat(rows.getString("name")).isEqualTo("Ada");
        }
    }
}
