package dev.vortex.iceberg.tasks;

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

    // Traverse all data files reachable from the current snapshot, convert them to Vortex
    // and write them into a new Vortex table.
    table
        .currentSnapshot()
        .addedDataFiles(table.io())
        .forEach(
            dataFile -> {
              log.info("Rewriting data file: {}", dataFile.location());
              try {
                DataFile newDataFile =
                    RewriteDataFile.of(
                            table.io(), dataFile, table.spec(), createTable.table().location())
                        .call();
                append.appendFile(newDataFile);
              } catch (Exception e) {
                throw new RuntimeException("Failed rewriting data file " + dataFile.location(), e);
              }
            });

    append.commit();
    createTable.commitTransaction();
  }
}
