package dev.vortex.iceberg.tasks;

import com.google.common.io.MoreFiles;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RewriteDataFile implements Callable<DataFile> {
  static final Logger log = LoggerFactory.getLogger(RewriteDataFile.class);
  static final Path TEMP_DIR;

  static {
    try {
      TEMP_DIR = java.nio.file.Files.createTempDirectory("rewrite");

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      MoreFiles.deleteRecursively(TEMP_DIR);
                    } catch (IOException e) {
                      log.warn("Failed to delete temp dir on exit", e);
                    }
                  }));

    } catch (IOException e) {
      throw new RuntimeException("failed creating temp dir", e);
    }
  }

  private final FileIO fileIO;
  private final DataFile dataFile;
  private final PartitionSpec partitionSpec;
  private final String outputLocation;

  RewriteDataFile(
      FileIO fileIO, DataFile dataFile, PartitionSpec partitionSpec, String outputLocation) {
    this.fileIO = fileIO;
    this.dataFile = dataFile;
    this.partitionSpec = partitionSpec;
    this.outputLocation = outputLocation;
  }

  public static RewriteDataFile of(
      FileIO fileIO, DataFile dataFile, PartitionSpec partitionSpec, String outputLocation) {
    return new RewriteDataFile(fileIO, dataFile, partitionSpec, outputLocation);
  }

  @Override
  public DataFile call() throws Exception {
    log.info("Rewriting data file: {}", dataFile.location());
    URI uri = URI.create(dataFile.location());
    String fileName = Paths.get(uri.getPath()).getFileName().toString();
    Path parquetFile = TEMP_DIR.resolve(fileName);

    try (SeekableInputStream stream = fileIO.newInputFile(dataFile).newStream()) {
      Files.copy(stream, parquetFile);
    }

    Path vortexFile = ConversionUtil.parquetToVortex(parquetFile);

    String location = resolveChild(outputLocation, "data/" + vortexFile.getFileName().toString());
    OutputFile outputFile = fileIO.newOutputFile(location);
    try (OutputStream outputStream = outputFile.createOrOverwrite()) {
      Files.copy(vortexFile, outputStream);
    }

    // Cleanup our space usage.
    Files.delete(parquetFile);
    Files.delete(vortexFile);

    return DataFiles.builder(partitionSpec)
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
