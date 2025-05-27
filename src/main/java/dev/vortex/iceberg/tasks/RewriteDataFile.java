package dev.vortex.iceberg.tasks;

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

public final class RewriteDataFile implements Callable<DataFile> {

  static final Path TEMP_DIR;

  static {
    try {
      TEMP_DIR = java.nio.file.Files.createTempDirectory("rewrite");
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
    URI uri = URI.create(dataFile.location());
    String fileName = Paths.get(uri.getPath()).getFileName().toString();
    Path file = TEMP_DIR.resolve(fileName);

    try (SeekableInputStream stream = fileIO.newInputFile(dataFile).newStream()) {
      Files.copy(stream, file);
    }

    Path vortexFile = ConversionUtil.parquetToVortex(file);
    String location = resolveChild(outputLocation, "data/" + vortexFile.getFileName().toString());
    OutputFile outputFile = fileIO.newOutputFile(location);
    try (OutputStream outputStream = outputFile.createOrOverwrite()) {
      Files.copy(vortexFile, outputStream);
    }
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
