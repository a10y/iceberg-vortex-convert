package dev.vortex.iceberg;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rewrite a single Iceberg table from a source to a destination location. */
public final class RewriteTable implements Runnable {
  static final Logger log = LoggerFactory.getLogger(RewriteTable.class);

  static final Path TEMP_DIR;

  static {
    try {
      TEMP_DIR = java.nio.file.Files.createTempDirectory("rewrite");
    } catch (IOException e) {
      throw new RuntimeException("failed creating temp dir", e);
    }
  }

  private final Catalog catalog;
  private final Table table;
  private final FileIO fileIO;

  RewriteTable(Catalog catalog, Table table, FileIO fileIO) {
    this.catalog = catalog;
    this.table = table;
    this.fileIO = fileIO;
  }

  public static RewriteTable of(Catalog catalog, Table table, FileIO fileIO) {
    return new RewriteTable(catalog, table, fileIO);
  }

  @Override
  public void run() {
    // Create a new table in the destination location.
    log.info("Rewriting table: " + table.name() + " to Vortex");
    Transaction createTable =
        catalog.newCreateTableTransaction(
            TableIdentifier.of(Namespace.of("vortex"), table.name()), table.schema(), table.spec());
    AppendFiles append = createTable.newAppend();

    table
        .currentSnapshot()
        .addedDataFiles(fileIO)
        .forEach(
            dataFile -> {
              log.info("rewriting data file: " + dataFile);
              append.appendFile(
                  rewriteDataFile(dataFile, table.spec(), createTable.table().location()));
            });

    append.commit();
    createTable.commitTransaction();
  }

  // Rewrite a single data file into Vortex.
  private DataFile rewriteDataFile(DataFile dataFile, PartitionSpec spec, String outputLocation) {
    URI uri = URI.create(dataFile.location());
    String fileName = Paths.get(uri.getPath()).getFileName().toString();
    Path file = TEMP_DIR.resolve(fileName);

    try (SeekableInputStream stream = fileIO.newInputFile(dataFile).newStream()) {
      Files.copy(stream, file);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Path vortexFile = ConversionUtil.parquetToVortex(file);
    String location = resolveChild(outputLocation, "data/" + vortexFile.getFileName().toString());
    OutputFile outputFile = fileIO.newOutputFile(location);
    try (OutputStream outputStream = outputFile.create()) {
      Files.copy(vortexFile, outputStream);
    } catch (IOException e) {
      throw new RuntimeException("newOutputStream", e);
    }

    return DataFiles.builder(spec)
        .withInputFile(outputFile.toInputFile())
        .withPartition(dataFile.partition())
        .withRecordCount(dataFile.recordCount())
        .withFormat(FileFormat.VORTEX)
        .build();
  }

  private String resolveChild(String outputLocation, String fileName) {
    if (!outputLocation.endsWith("/")) {
      outputLocation = outputLocation + "/";
    }

    return URI.create(outputLocation).resolve(fileName).toString();
  }
}
