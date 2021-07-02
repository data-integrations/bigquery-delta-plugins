/*
 *
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.delta.bigquery;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.base.Strings;
import io.cdap.delta.api.SourceTable;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;

/**
 * Tests for BigQueryUtils. For tests in "BigQueryGCPDependentTests"  service account credentials must be set in the
 * system properties. The service account must have permission to create and write to BigQuery datasets and tables.
 *
 * The tests create real resources in GCP and will cost some small amount of money for each run.
 */
@RunWith(Enclosed.class)
public class BigQueryUtilsTest {

  private static final String DATASET = "demodataset";
  private static final String TABLE_PREFIX = "demotable_";

  @PrepareForTest(BigQueryUtils.class)
  @RunWith(PowerMockRunner.class)
  public static class LocalIndependentTests {
    private BigQuery bigQueryMock;
    private Table tableMock;

    @Before
    public void init() throws Exception {
      //Mocks
      bigQueryMock = Mockito.mock(BigQuery.class);
      tableMock = Mockito.mock(Table.class);
      Mockito.when(bigQueryMock.getTable(ArgumentMatchers.any())).thenReturn(tableMock);
      PowerMockito.spy(BigQueryUtils.class);

      //Stubs
      PowerMockito.doReturn(1L, 2L, 3L, 4L)
        .when(BigQueryUtils.class, "executeAggregateQuery",
              ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    public void testNormalizeDataSetOrTableName() {
      // only contains number and letter
      assertEquals("a2fs", BigQueryUtils.normalizeDatasetOrTableName("a2fs"));
      // only contains number and letter start with number
      assertEquals("2fas", BigQueryUtils.normalizeDatasetOrTableName("2fas"));
      // only contains number and letter and length is 1024
      String name = Strings.repeat("a1", 512);
      assertEquals(name, BigQueryUtils.normalizeDatasetOrTableName(name));
      // only contains number and letter and length is 1026
      name = Strings.repeat("a1", 513);
      assertEquals(name.substring(0, 1024), BigQueryUtils.normalizeDatasetOrTableName(name));
      // contains invalid character
      assertEquals("ab_c", BigQueryUtils.normalizeDatasetOrTableName("ab?/c"));
      // contains space
      assertEquals("a2_fs", BigQueryUtils.normalizeFieldName("a2 fs"));

    }

    @Test
    public void testNormalizeFieldName() {
      // only contains number and letter
      assertEquals("a2fs", BigQueryUtils.normalizeFieldName("a2fs"));
      // only contains number and letter start with number
      assertEquals("_2fas", BigQueryUtils.normalizeFieldName("2fas"));
      // only contains number and letter and length is 128
      String name = Strings.repeat("a1", 64);
      assertEquals(name, BigQueryUtils.normalizeFieldName(name));
      // only contains number and letter, starts with number and length is 128
      name = Strings.repeat("1a", 64);
      assertEquals("_" + name.substring(0, 127), BigQueryUtils.normalizeFieldName(name));
      // only contains number and letter and length is 130
      name = Strings.repeat("a1", 65);
      assertEquals(name.substring(0, 128), BigQueryUtils.normalizeFieldName(name));
      // contains invalid character
      assertEquals("ab_c", BigQueryUtils.normalizeFieldName("ab?/c"));
      // contains space
      assertEquals("a2_fs", BigQueryUtils.normalizeFieldName("a2 fs"));
    }


    @Test
    public void testGetMaximumExistingSequenceNumberZeroInvocations() throws Exception {
      // Zero Tables
      Set<SourceTable> allTables = generateSourceTableSet(0);
      long tableResult0 = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(allTables, "testproject",
                                                                         null, bigQueryMock, null, 1000);
      assertEquals(0L, tableResult0);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(0))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

    }

    @Test
    public void testGetMaximumExistingSequenceNumberSingleInvocations() throws Exception {

      // Subtest : One Table
      Set<SourceTable> allTables = generateSourceTableSet(1);
      long tableResult = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(allTables, "testproject",
                                                                        null, bigQueryMock, null, 1000);
      assertEquals(1L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(1))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

      // Subtest2 : Ten Tables
      allTables = generateSourceTableSet(10);
      tableResult = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(allTables, "testproject",
                                                                   null, bigQueryMock, null, 1000);
      assertEquals(2L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(2))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

      // Subtest3 : 1000 Tables
      allTables = generateSourceTableSet(1000);
      tableResult = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(allTables, "testproject",
                                                                   null, bigQueryMock, null, 1000);
      assertEquals(3L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(3))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

    }

    @Test
    public void testGetMaximumExistingSequenceNumberDoubleInvocations() throws Exception {

      //Subtest1 :  1001 Tables : Should call bigquery 2 times. 1000+1
      Set<SourceTable> allTables = generateSourceTableSet(1001);
      long tableResult = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(allTables, "testproject",
                                                                        null, bigQueryMock, null, 1000);
      assertEquals(2L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(2))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

      //Subtest2 :  2000 Tables : Should call bigquery 2 times. 1000+1000
      allTables = generateSourceTableSet(2000);
      tableResult = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(allTables, "testproject",
                                                                   null, bigQueryMock, null, 1000);
      assertEquals(4L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(4))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

    }

    @Test
    public void testGetMaximumExistingSequenceNumberTripleInvocations() throws Exception {

      //Subtest1 :  2500 Tables : Should call bigquery 3 times. 1000+1000+500
      Set<SourceTable> allTables = generateSourceTableSet(2500);
      long tableResult = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(allTables, "testproject",
                                                                        null, bigQueryMock, null, 1000);
      assertEquals(3L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(3))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

    }


  }

  public static class BigQueryGCPDependentTests {

    private static final Schema DEMOTABLE_SCHEMA = Schema.of(
      Field.of("_sequence_num", LegacySQLTypeName.INTEGER),
      Field.of("id", LegacySQLTypeName.INTEGER),
      Field.of("name", LegacySQLTypeName.STRING));
    private static BigQuery bigQuery;
    private static String project;
    private static final int MAX_BIG_QUERY_BATCH_SZE = 4;

    @BeforeClass
    public static void setupTestClass() throws Exception {
      project = System.getProperty("project.id");
      Assume.assumeNotNull(project);
      String serviceAccountFilePath = System.getProperty("service.account.file");
      String serviceAccountContent = System.getProperty("service.account.content");
      Assume.assumeTrue(serviceAccountFilePath != null || serviceAccountContent != null);

      Credentials credentials;
      if (serviceAccountContent != null) {
        try (InputStream is = new ByteArrayInputStream(serviceAccountContent.getBytes(StandardCharsets.UTF_8))) {
          credentials = GoogleCredentials.fromStream(is)
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
        }
      } else {
        File serviceAccountFile = new File(serviceAccountFilePath);
        try (InputStream is = new FileInputStream(serviceAccountFile)) {
          credentials = GoogleCredentials.fromStream(is)
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
        }
      }

      bigQuery = BigQueryOptions.newBuilder()
        .setCredentials(credentials)
        .setProjectId(project)
        .build()
        .getService();

      generateBigQueryTestTables();
    }

    private static void generateBigQueryTestTables() throws Exception {
      // Create Dataset
      bigQuery.create(DatasetInfo.newBuilder(DATASET).build());

      for (int i = 1; i <= 10; i++) {
        // Create Table
        String tableName = TABLE_PREFIX + i;
        TableId tableId = TableId.of(DATASET, tableName);
        TableDefinition tableDefinition = StandardTableDefinition.of(DEMOTABLE_SCHEMA);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);

        //Insert data
        String query = String.format("INSERT %s.%s (_sequence_num, id, name) VALUES" +
                                       "(0, 1,'John'), " +
                                       "(%s, 2,'Snow')", DATASET, tableName, i, i);
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        bigQuery.query(queryConfig);

      }
    }

    @AfterClass
    public static void cleanup() throws Exception {
      //delete dataset
      bigQuery.delete(DatasetId.of(project, DATASET), BigQuery.DatasetDeleteOption.deleteContents());
    }

    @Test
    public void testMaximumExistingSequenceNumber() {
      //0 tables : no call
      Set<SourceTable> tables0 = generateSourceTableSet(0);
      long tableResult0 = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(tables0, project,
                                                                         DATASET, bigQuery, null,
                                                                         MAX_BIG_QUERY_BATCH_SZE);
      assertEquals(0, tableResult0);

      //3 tables : 1 call
      Set<SourceTable> tables3 = generateSourceTableSet(3);
      long tableResult3 = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(tables3, project,
                                                                         DATASET, bigQuery, null,
                                                                         MAX_BIG_QUERY_BATCH_SZE);
      assertEquals(3, tableResult3);

      //6 tables : 2 call
      Set<SourceTable> tables6 = generateSourceTableSet(6);
      long tableResult6 = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(tables6, project,
                                                                         DATASET, bigQuery, null,
                                                                         MAX_BIG_QUERY_BATCH_SZE);
      assertEquals(6, tableResult6);

      //10 tables : 3 call
      Set<SourceTable> tables10 = generateSourceTableSet(10);
      long tableResult10 = BigQueryUtils.getMaximumExistingSequenceNumberBatchSpliter(tables10, project,
                                                                          DATASET, bigQuery, null,
                                                                          MAX_BIG_QUERY_BATCH_SZE);
      assertEquals(10, tableResult10);
    }

  }


  // Helpers Functions :
  private static Set<SourceTable> generateSourceTableSet(int noOfTables) {
    Set<SourceTable> allTables = new HashSet<>();
    for (int i = 0; i < noOfTables; i++) {
      allTables.add(new SourceTable(DATASET, TABLE_PREFIX + (i + 1),
                                    null, null, new HashSet<>(), new HashSet<>()));
    }
    return allTables;
  }
}
