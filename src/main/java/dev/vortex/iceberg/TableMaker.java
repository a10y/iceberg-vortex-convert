package dev.vortex.iceberg;

import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "table-maker", mixinStandardHelpOptions = true)
public final class TableMaker implements Callable<Integer> {

  @Override
  public Integer call() throws Exception {
    return 0;
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new TableMaker()).execute(args));
  }
}
