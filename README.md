## building

This requires that you have checked out the Iceberg fork locally and have published it to mavenLocal:

```
git clone https://github.com/spiraldb/iceberg.git
cd iceberg && ./gradlew publishToMavenLocal
```

Then you can build the fat JAR for this repository, which uses several of the locally published artifacts:

```
mvn clean package
```

## running

```
java -jar ./target/vortex-table-maker-1.0-SNAPSHOT-jar-with-dependencies.jar --help
```

An example with all arguments:

```
java -jar ./target/vortex-table-maker-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --access-key=REDACTED \
    --warehouse="abfss://CONTAINER@ACCOUNT.dfs.core.windows.net/run/iceberg" \
    customer lineitem partsupp
```
