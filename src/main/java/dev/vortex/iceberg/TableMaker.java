package dev.vortex.iceberg;

import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
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

  @Override
  public Integer call() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.azure.account.key.formatsscaletest.blob.core.windows.net", accessKey);
    try (HadoopCatalog catalog = new HadoopCatalog(conf, warehouse)) {
      Schema schema = catalog.loadTable(TableIdentifier.of("call_center")).schema();
      System.out.println("Schema: " + schema);
    }

    return 0;
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new TableMaker()).execute(args));
  }
}
