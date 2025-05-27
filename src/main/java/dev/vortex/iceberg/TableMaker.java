package dev.vortex.iceberg;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "table-maker", mixinStandardHelpOptions = true)
public final class TableMaker implements Callable<Integer> {

  @Option(
      names = {"--access-key"},
      required = true,
      description = "Azure account key")
  private String accessKey;

  @Option(
      names = {"--warehouse"},
      required = true,
      description = "Iceberg warehouse location")
  private String warehouse;

  @Option(names = "--tables", required = true, defaultValue = "call_center")
  private List<String> tables;

  @Override
  public Integer call() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.azure.account.key", accessKey);

    LoggerFactory.getLogger(TableMaker.class).info("Using warehouse: " + warehouse);

    try (HadoopCatalog catalog = new HadoopCatalog(conf, warehouse);
        FileIO fileIO = new HadoopFileIO(conf)) {
      for (String tableName : tables) {
        Table table = catalog.loadTable(TableIdentifier.of(Namespace.empty(), tableName));
        RewriteTable.of(catalog, table, fileIO).run();
      }
    }

    return 0;
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new TableMaker()).execute(args));
  }
}
