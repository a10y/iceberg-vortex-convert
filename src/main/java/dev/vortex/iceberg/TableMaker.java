package dev.vortex.iceberg;

import dev.vortex.iceberg.tasks.RewriteTable;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "iceberg-vortex-convert", mixinStandardHelpOptions = true)
public final class TableMaker implements Callable<Integer> {

  static final Logger log = LoggerFactory.getLogger(TableMaker.class);

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

  @Parameters(
      description = "The Iceberg tables to convert into Vortex",
      defaultValue = "call_center")
  private List<String> tables;

  @Override
  public Integer call() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.azure.account.key", accessKey);

    log.info("Using warehouse {}", warehouse);

    try (HadoopCatalog catalog = new HadoopCatalog(conf, warehouse)) {
      for (String tableName : tables) {
        Table table = catalog.loadTable(TableIdentifier.of(tableName));
        RewriteTable.of(tableName, catalog, table).run();
      }
    }

    return 0;
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new TableMaker()).execute(args));
  }
}
