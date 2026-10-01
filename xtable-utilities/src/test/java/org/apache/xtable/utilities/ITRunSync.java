/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.utilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestJavaHudiTable;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.model.storage.TableFormat;

class ITRunSync {

  @Test
  void testSingleSyncMode(@TempDir Path tempDir) throws IOException {
    String tableName = "test-table";
    try (GenericTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRows(20);
      File configFile = writeConfigFile(tempDir, table, tableName);
      String[] args = new String[] {"--datasetConfig", configFile.getPath()};
      RunSync.main(args);
      Path icebergMetadataPath = Paths.get(URI.create(table.getBasePath() + "/metadata"));
      waitForNumIcebergCommits(icebergMetadataPath, 3);
    }
  }

  @Test
  void testContinuousSyncMode(@TempDir Path tempDir) throws IOException {
    ExecutorService runner = Executors.newSingleThreadExecutor();
    String tableName = "test-table";
    try (GenericTable table =
        TestJavaHudiTable.forStandardSchemaWithFieldIds(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRows(20);
      File configFile = writeConfigFile(tempDir, table, tableName);
      String[] args = new String[] {"--datasetConfig", configFile.getPath(), "--continuousMode"};
      runner.submit(
          () -> {
            try {
              RunSync.main(args);
            } catch (IOException ex) {
              throw new UncheckedIOException(ex);
            }
          });
      Path icebergMetadataPath = Paths.get(URI.create(table.getBasePath() + "/metadata"));
      waitForNumIcebergCommits(icebergMetadataPath, 3);
    }
    try (GenericTable table =
        TestJavaHudiTable.withAdditionalColumnsAndFieldIds(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      // write more data now that table is initialized and data is synced
      table.insertRows(20);
      Path icebergMetadataPath = Paths.get(URI.create(table.getBasePath() + "/metadata"));
      waitForNumIcebergCommits(icebergMetadataPath, 6);
      assertEquals(6, numIcebergMetadataJsonFiles(icebergMetadataPath));
    } finally {
      runner.shutdownNow();
    }
  }

  @Test
  void testIcebergSourceToHudiTargetWithLongPartitionColumn(@TempDir Path tempDir)
      throws IOException {
    String tableName = "iceberg-long-partition";
    String basePath = tempDir.resolve(tableName).toString();
    String dataPath = basePath + "/data";
    List<String> expectedRows = writeIcebergTablePartitionedByLong(basePath, 3, 5);

    RunSync.DatasetConfig config =
        RunSync.DatasetConfig.builder()
            .sourceFormat(TableFormat.ICEBERG)
            .targetFormats(Collections.singletonList(TableFormat.HUDI))
            .datasets(
                Collections.singletonList(
                    RunSync.DatasetConfig.Table.builder()
                        .tableBasePath(basePath)
                        .tableDataPath(dataPath)
                        .tableName(tableName)
                        .build()))
            .build();
    File configFile = writeConfigFile(tempDir, config);
    RunSync.main(new String[] {"--datasetConfig", configFile.getPath()});

    // Hudi partition paths are relative to the Hudi base path, so the Hudi table must be rooted at
    // the Iceberg data path for "_sys_parted_by=<n>" to parse as a single partition value.
    assertTrue(Files.exists(Paths.get(dataPath, ".hoodie")), "Hudi table not created at data path");
    try (SparkSession spark =
        SparkSession.builder()
            .config(HoodieReadClient.addHoodieSupport(HudiTestUtil.getSparkConf(tempDir)))
            .getOrCreate()) {
      List<String> hudiRows =
          spark
              .read()
              .format("hudi")
              .option(HoodieMetadataConfig.ENABLE.key(), "true")
              .option("hoodie.datasource.read.extract.partition.values.from.path", "true")
              .load(dataPath)
              .selectExpr("id", "_sys_parted_by")
              .orderBy("id")
              .toJSON()
              .collectAsList();
      assertEquals(expectedRows, hudiRows);
    }
  }

  /**
   * Writes an Iceberg table identity-partitioned by a LONG column, with data files under {@code
   * <basePath>/data/_sys_parted_by=<n>/}. Returns the expected rows as JSON, ordered by id.
   */
  private static List<String> writeIcebergTablePartitionedByLong(
      String basePath, int numPartitions, int rowsPerPartition) throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "_sys_parted_by", Types.LongType.get()),
            Types.NestedField.optional(3, "name", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("_sys_parted_by").build();
    Table table = new HadoopTables(new Configuration()).create(schema, spec, basePath);
    AppendFiles append = table.newAppend();
    List<String> expectedRows = new ArrayList<>();
    long id = 0;
    for (long partition = 0; partition < numPartitions; partition++) {
      List<Record> records = new ArrayList<>();
      for (int i = 0; i < rowsPerPartition; i++, id++) {
        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("_sys_parted_by", partition);
        record.setField("name", "row-" + id);
        records.add(record);
        expectedRows.add(String.format("{\"id\":%d,\"_sys_parted_by\":%d}", id, partition));
      }
      PartitionKey partitionKey = new PartitionKey(spec, schema);
      partitionKey.partition(records.get(0));
      String filePath =
          String.format(
              "%s/data/%s/%s.parquet", basePath, partitionKey.toPath(), UUID.randomUUID());
      DataWriter<Record> writer =
          Parquet.writeData(table.io().newOutputFile(filePath))
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::create)
              .withSpec(spec)
              .withPartition(partitionKey)
              .build();
      try {
        records.forEach(writer::write);
      } finally {
        writer.close();
      }
      append.appendFile(writer.toDataFile());
    }
    append.commit();
    return expectedRows;
  }

  private static File writeConfigFile(Path tempDir, GenericTable table, String tableName)
      throws IOException {
    RunSync.DatasetConfig config =
        RunSync.DatasetConfig.builder()
            .sourceFormat("HUDI")
            .targetFormats(Collections.singletonList("ICEBERG"))
            .datasets(
                Collections.singletonList(
                    RunSync.DatasetConfig.Table.builder()
                        .tableBasePath(table.getBasePath())
                        .tableName(tableName)
                        .build()))
            .build();
    return writeConfigFile(tempDir, config);
  }

  private static File writeConfigFile(Path tempDir, RunSync.DatasetConfig config)
      throws IOException {
    File configFile = new File(tempDir + "config.yaml");
    RunSync.YAML_MAPPER.writeValue(configFile, config);
    return configFile;
  }

  @SneakyThrows
  private static void waitForNumIcebergCommits(Path metadataPath, int count) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < TimeUnit.MINUTES.toMillis(5)) {
      if (numIcebergMetadataJsonFiles(metadataPath) == count) {
        break;
      }
      Thread.sleep(5000);
    }
  }

  @SneakyThrows
  private static long numIcebergMetadataJsonFiles(Path path) {
    long count = 0;
    if (Files.exists(path)) {
      count = Files.list(path).filter(p -> p.toString().endsWith("metadata.json")).count();
    }
    return count;
  }
}
