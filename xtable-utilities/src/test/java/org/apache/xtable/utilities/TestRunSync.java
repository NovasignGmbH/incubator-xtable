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

import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;
import static org.apache.xtable.model.storage.TableFormat.PAIMON;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.xtable.conversion.CatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.utilities.RunSync.DatasetConfig;
import org.apache.xtable.utilities.RunSync.TableFormatConverters;
import org.apache.xtable.utilities.RunSync.TableFormatConverters.ConversionConfig;

class TestRunSync {

  @Test
  public void testMain() {
    String filePath = TestRunSync.class.getClassLoader().getResource("my_config.yaml").getPath();
    String[] args = new String[] {"--datasetConfig", filePath};
    Assertions.assertDoesNotThrow(
        () -> RunSync.main(args), "RunSync.main() threw an unexpected exception.");
  }

  @Test
  public void testGetDatasetConfigWithNonExistentFile() {
    URL resourceUrl = TestRunSync.class.getClassLoader().getResource("my_config1.yaml");
    Assertions.assertNull(resourceUrl, "Config file not found in classpath");
  }

  @Test
  public void testGetDatasetConfigWithValidYAML() throws IOException {
    String filePath = TestRunSync.class.getClassLoader().getResource("my_config.yaml").getPath();
    DatasetConfig config = RunSync.getDatasetConfig(filePath);
    // Assert
    Assertions.assertNotNull(config);
  }

  /** Tests that the default hadoop configs are loaded. */
  @Test
  public void testLoadDefaultHadoopConfig() {
    Configuration conf;
    conf = new Configuration();
    String value = conf.get("fs.file.impl");
    Assertions.assertNull(value);

    conf = RunSync.loadHadoopConf(null);
    value = conf.get("fs.file.impl");
    Assertions.assertEquals("org.apache.hadoop.fs.LocalFileSystem", value);
  }

  /** Tests that the custom hadoop configs are loaded and can override defaults. */
  @Test
  public void testLoadCustomHadoopConfig() {
    Configuration conf;
    conf = new Configuration();
    String value = conf.get("fs.azure.account.oauth2.client.endpoint");
    Assertions.assertNull(value);

    // build a custom hadoop config
    String customXmlConfig =
        "<configuration>"
            + "  <property>"
            + "    <name>fs.file.impl</name>"
            + "    <value>override_default_value</value>"
            + "  </property>"
            + "  <property>"
            + "    <name>fs.azure.account.oauth2.client.endpoint</name>"
            + "    <value>https://login.microsoftonline.com/</value>"
            + "  </property>"
            + "</configuration>";

    conf = RunSync.loadHadoopConf(customXmlConfig.getBytes());
    value = conf.get("fs.file.impl");
    Assertions.assertEquals("override_default_value", value);
    value = conf.get("fs.azure.account.oauth2.client.endpoint");
    Assertions.assertEquals("https://login.microsoftonline.com/", value);
  }

  @Test
  public void testTableFormatConverterConfigDefault() throws IOException {
    TableFormatConverters converters = RunSync.loadTableFormatConversionConfigs(null);
    Map<String, ConversionConfig> tfConverters = converters.getTableFormatConverters();
    Assertions.assertEquals(4, tfConverters.size());
    Assertions.assertNotNull(tfConverters.get(DELTA));
    Assertions.assertNotNull(tfConverters.get(HUDI));
    Assertions.assertNotNull(tfConverters.get(ICEBERG));
    Assertions.assertNotNull(tfConverters.get(PAIMON));

    Assertions.assertEquals(
        "org.apache.xtable.hudi.HudiConversionSourceProvider",
        tfConverters.get(HUDI).getConversionSourceProviderClass());
    Assertions.assertEquals(
        "org.apache.xtable.hudi.HudiConversionTarget",
        tfConverters.get(HUDI).getConversionTargetProviderClass());
    Assertions.assertEquals(
        "org.apache.xtable.iceberg.IcebergConversionTarget",
        tfConverters.get(ICEBERG).getConversionTargetProviderClass());
    Assertions.assertEquals(
        "org.apache.xtable.iceberg.IcebergConversionSourceProvider",
        tfConverters.get(ICEBERG).getConversionSourceProviderClass());
    Assertions.assertEquals(
        "org.apache.xtable.delta.DeltaConversionTarget",
        tfConverters.get(DELTA).getConversionTargetProviderClass());
    Assertions.assertEquals(
        "org.apache.xtable.delta.DeltaConversionSourceProvider",
        tfConverters.get(DELTA).getConversionSourceProviderClass());
    Assertions.assertEquals(
        "org.apache.xtable.paimon.PaimonConversionSourceProvider",
        tfConverters.get(PAIMON).getConversionSourceProviderClass());
  }

  @Test
  public void testTableFormatConverterCustom() throws IOException {
    String customConverters =
        "tableFormatConverters:\n"
            + "  HUDI:\n"
            + "    conversionSourceProviderClass: foo\n"
            + "  DELTA:\n"
            + "    configuration:\n"
            + "      spark.master: local[4]\n"
            + "      foo: bar\n"
            + "  NEW_FORMAT:\n"
            + "    conversionSourceProviderClass: bar\n";
    TableFormatConverters converters =
        RunSync.loadTableFormatConversionConfigs(customConverters.getBytes());
    Map<String, ConversionConfig> tfConverters = converters.getTableFormatConverters();
    Assertions.assertEquals(5, tfConverters.size());

    Assertions.assertNotNull(tfConverters.get("NEW_FORMAT"));
    Assertions.assertEquals(
        "bar", tfConverters.get("NEW_FORMAT").getConversionSourceProviderClass());

    Assertions.assertEquals("foo", tfConverters.get(HUDI).getConversionSourceProviderClass());

    Map<String, String> deltaConfigs = tfConverters.get(DELTA).getConfiguration();
    Assertions.assertEquals(3, deltaConfigs.size());
    Assertions.assertEquals("local[4]", deltaConfigs.get("spark.master"));
    Assertions.assertEquals("bar", deltaConfigs.get("foo"));
  }

  @Test
  public void testIcebergCatalogConfig() throws IOException {
    String icebergConfig =
        "catalogImpl: org.apache.xtable.CatalogImpl\n"
            + "catalogName: test\n"
            + "catalogOptions: \n"
            + "  option1: value1\n"
            + "  option2: value2";
    CatalogConfig catalogConfig = RunSync.loadIcebergCatalogConfig(icebergConfig.getBytes());
    Assertions.assertEquals("org.apache.xtable.CatalogImpl", catalogConfig.getCatalogImpl());
    Assertions.assertEquals("test", catalogConfig.getCatalogName());
    Assertions.assertEquals(2, catalogConfig.getCatalogOptions().size());
    Assertions.assertEquals("value1", catalogConfig.getCatalogOptions().get("option1"));
    Assertions.assertEquals("value2", catalogConfig.getCatalogOptions().get("option2"));
  }

  private static final String BASE_PATH = "file:///tmp/test-table";
  private static final String DATA_PATH = BASE_PATH + "/data";

  @Test
  public void testTargetTableBasePath() {
    // {source format, target format, expected target base path}. Hudi partition paths are
    // relative to the Hudi base path, so an Iceberg source places the Hudi target at the data path.
    String[][] cases = {
      {ICEBERG, HUDI, DATA_PATH},
      {ICEBERG, DELTA, BASE_PATH},
      {HUDI, ICEBERG, BASE_PATH},
      {HUDI, DELTA, BASE_PATH},
      {DELTA, HUDI, BASE_PATH},
      {DELTA, ICEBERG, BASE_PATH},
      {PAIMON, HUDI, BASE_PATH}
    };
    DatasetConfig.Table table =
        DatasetConfig.Table.builder()
            .tableBasePath(BASE_PATH)
            .tableDataPath(DATA_PATH)
            .tableName("test-table")
            .namespace("db.schema")
            .build();
    for (String[] testCase : cases) {
      SourceTable sourceTable = buildSourceTable(table, testCase[0]);

      List<TargetTable> targetTables =
          RunSync.targetTableBuilder(sourceTable, null, Collections.singletonList(testCase[1]));

      String message = testCase[0] + " -> " + testCase[1];
      Assertions.assertEquals(1, targetTables.size(), message);
      TargetTable targetTable = targetTables.get(0);
      Assertions.assertEquals(testCase[2], targetTable.getBasePath(), message);
      Assertions.assertEquals(testCase[1], targetTable.getFormatName(), message);
      Assertions.assertEquals("test-table", targetTable.getName(), message);
      Assertions.assertArrayEquals(
          new String[] {"db", "schema"}, targetTable.getNamespace(), message);
    }
  }

  @Test
  public void testTargetTableBasePathPerTargetFormat() {
    DatasetConfig.Table table =
        DatasetConfig.Table.builder()
            .tableBasePath(BASE_PATH)
            .tableDataPath(DATA_PATH)
            .tableName("test-table")
            .build();
    SourceTable sourceTable = buildSourceTable(table, ICEBERG);

    List<TargetTable> targetTables =
        RunSync.targetTableBuilder(sourceTable, null, Arrays.asList(HUDI, DELTA));

    Assertions.assertEquals(DATA_PATH, targetTables.get(0).getBasePath());
    Assertions.assertEquals(BASE_PATH, targetTables.get(1).getBasePath());
  }

  @Test
  public void testTargetTableBasePathWithoutDataPath() {
    DatasetConfig.Table table =
        DatasetConfig.Table.builder().tableBasePath(BASE_PATH).tableName("test-table").build();
    SourceTable sourceTable = buildSourceTable(table, ICEBERG);

    List<TargetTable> targetTables =
        RunSync.targetTableBuilder(sourceTable, null, Collections.singletonList(HUDI));

    Assertions.assertEquals(BASE_PATH, targetTables.get(0).getBasePath());
  }

  private static SourceTable buildSourceTable(DatasetConfig.Table table, String sourceFormat) {
    DatasetConfig datasetConfig =
        DatasetConfig.builder()
            .sourceFormat(sourceFormat)
            .datasets(Collections.singletonList(table))
            .build();
    return RunSync.sourceTableBuilder(table, null, datasetConfig, new Properties());
  }
}
