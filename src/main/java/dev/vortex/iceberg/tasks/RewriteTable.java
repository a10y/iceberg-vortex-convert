package dev.vortex.iceberg.tasks;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rewrite a single Iceberg table from a source to a destination location. */
public final class RewriteTable implements Runnable {
  static final Logger log = LoggerFactory.getLogger(RewriteTable.class);

  private final Catalog catalog;
  private final Table table;
  private final String tableName;

  RewriteTable(String tableName, Catalog catalog, Table table) {
    this.tableName = tableName;
    this.catalog = catalog;
    this.table = table;
  }

  public static RewriteTable of(String tableName, Catalog catalog, Table table) {
    return new RewriteTable(tableName, catalog, table);
  }

  @Override
  public void run() {
    // Create a new table in the destination location.
    log.info("Rewriting table: {} to Vortex", tableName);
    Transaction createTable =
        catalog.newReplaceTableTransaction(
            TableIdentifier.of(Namespace.of("vortex"), tableName),
            table.schema(),
            table.spec(),
            true);
    AppendFiles append = createTable.newAppend();

    // ForkJoin to rewrite in parallel.
    ExecutorService executor = Executors.newFixedThreadPool(4);

    List<Callable<DataFile>> rewriteTasks =
        ImmutableList.copyOf(table.currentSnapshot().addedDataFiles(table.io())).stream()
            .map(
                dataFile ->
                    RewriteDataFile.of(
                        table.io(), dataFile, table.spec(), createTable.table().location()))
            .collect(Collectors.toList());

    try {
      log.info("Begin {} parallel rewrite tasks", rewriteTasks.size());
      List<Future<DataFile>> dataFiles = executor.invokeAll(rewriteTasks);
      dataFiles.forEach(fut -> append.appendFile(Futures.getUnchecked(fut)));

      append.commit();
      createTable.commitTransaction();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted invoking rewrite futures for table " + tableName, e);
    }

    executor.shutdown();
  }
}
