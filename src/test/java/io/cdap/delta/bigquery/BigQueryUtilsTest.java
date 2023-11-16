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

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
  private static final String PROJECT = "testproject";

  @PrepareForTest(BigQueryUtils.class)
  @RunWith(PowerMockRunner.class)
  public static class LocalIndependentTests {
    private BigQuery bigQueryMock;

    @Before
    public void init() throws Exception {
      //Mocks
      bigQueryMock = Mockito.mock(BigQuery.class);
      Table tableMock = Mockito.mock(Table.class);
      Dataset datasetMock = Mockito.mock(Dataset.class);
      Mockito.when(bigQueryMock.getTable(ArgumentMatchers.any())).thenReturn(tableMock);
      Mockito.when(bigQueryMock.getDataset("demodataset")).thenReturn(datasetMock);
      PowerMockito.spy(BigQueryUtils.class);

      //Stubs
      PowerMockito.doReturn(1L, 2L, 3L, 4L)
        .when(BigQueryUtils.class, "executeAggregateQuery",
              ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    public void testGetNormalizeDatasetName() {
      // only contains number and letter
      assertEquals("a2fs", BigQueryUtils.getNormalizedDatasetName("a2fs", "db2"));
      assertEquals("db2", BigQueryUtils.getNormalizedDatasetName(null, "db2"));
      assertEquals("db2", BigQueryUtils.getNormalizedDatasetName("", "db2"));

      // only contains number and letter start with number
      assertEquals("2fas", BigQueryUtils.getNormalizedDatasetName("2fas", "db2"));

      // contains invalid character
      assertEquals("ab_c", BigQueryUtils.getNormalizedDatasetName("ab?/c", "db?/c"));
      assertEquals("db_c", BigQueryUtils.getNormalizedDatasetName(null, "db?/c"));
      assertEquals("db_c", BigQueryUtils.getNormalizedDatasetName("", "db?/c"));
    }

    @Test
    public void testNormalizeDatasetName() {
      // only contains number and letter
      assertEquals("a2fs", BigQueryUtils.normalizeDatasetName("a2fs"));
      // only contains number and letter start with number
      assertEquals("2fas", BigQueryUtils.normalizeDatasetName("2fas"));
      // only contains number and letter and length is 1024
      String name = Strings.repeat("a1", 512);
      assertEquals(name, BigQueryUtils.normalizeDatasetName(name));
      // only contains number and letter and length is 1026
      name = Strings.repeat("a1", 513);
      assertEquals(name.substring(0, 1024), BigQueryUtils.normalizeDatasetName(name));
      // contains invalid character
      assertEquals("ab_c", BigQueryUtils.normalizeDatasetName("ab?/c"));
      // contains space (invalid character)
      assertEquals("a2_fs", BigQueryUtils.normalizeDatasetName("a2 fs"));
      // contains hyphen (invalid character)
      assertEquals("a2_fs", BigQueryUtils.normalizeDatasetName("a2-fs"));
    }

    @Test
    public void testNormalizeTableName() {
      // only contains number and letter
      assertEquals("a2fs", BigQueryUtils.normalizeTableName("a2fs"));
      // only contains number and letter start with number
      assertEquals("2fas", BigQueryUtils.normalizeTableName("2fas"));
      // only contains number and letter and length is 1024
      String name = Strings.repeat("a1", 512);
      assertEquals(name, BigQueryUtils.normalizeTableName(name));
      // only contains number and letter and length is 1026
      name = Strings.repeat("a1", 513);
      assertEquals(name.substring(0, 1024), BigQueryUtils.normalizeTableName(name));
      // contains invalid character
      assertEquals("ab_c", BigQueryUtils.normalizeTableName("ab?c"));
      // contains space (this is valid)
      assertEquals("a2 fs", BigQueryUtils.normalizeTableName("a2 fs"));
      // contains hyphen (this is valid)
      assertEquals("a2-fs", BigQueryUtils.normalizeTableName("a2-fs"));
    }

    @Test
    public void testNormalizeFieldName() {
      int maxColumnNameLength = 300;
      String prefix = "_";
      // only contains number and letter
      assertEquals("a2fs", BigQueryUtils.normalizeFieldName("a2fs", false));
      // only contains number and letter start with number
      assertEquals("_2fas", BigQueryUtils.normalizeFieldName("2fas", false));
      // only contains number and letter and length is 300
      String name = Strings.repeat("a1", 150);
      assertEquals(name, BigQueryUtils.normalizeFieldName(name, false));
      // only contains number and letter, starts with number and length is 300
      name = Strings.repeat("1a", 150);
      assertEquals(prefix + name.substring(0, maxColumnNameLength - prefix.length()),
              BigQueryUtils.normalizeFieldName(name, false));
      // only contains number and letter and length is 130
      name = Strings.repeat("a1", 151);
      assertEquals(name.substring(0, maxColumnNameLength), BigQueryUtils.normalizeFieldName(name, false));
      // contains invalid character
      assertEquals("ab_c", BigQueryUtils.normalizeFieldName("ab?/c", false));
      // contains space
      assertEquals("a2_fs", BigQueryUtils.normalizeFieldName("a2 fs", false));
      // contains hyphen
      assertEquals("a2-fs", BigQueryUtils.normalizeFieldName("a2-fs", true));
      // contains chinese character
      assertEquals("你好世界", BigQueryUtils.normalizeFieldName("你好世界", true));
      // contains japanese character
      assertEquals("こんにちは世界", BigQueryUtils.normalizeFieldName("こんにちは世界", true));
      // contains emoji
      assertEquals("_", BigQueryUtils.normalizeFieldName("👍", true));

      // Testing valid characters

      // underscore is a valid character
      assertEquals("valid_", BigQueryUtils.normalizeFieldName("valid_", true));
      // space is a valid character
      assertEquals("Space is valid", BigQueryUtils.normalizeFieldName("Space is valid", true));
      // ampersand is valid
      assertEquals("ampersand&", BigQueryUtils.normalizeFieldName("ampersand&", true));
      // percent is valid and not replaced
      assertEquals("percent%", BigQueryUtils.normalizeFieldName("percent%", true));
      // equals is valid and not replaced
      assertEquals("equals=", BigQueryUtils.normalizeFieldName("equals=", true));
      // plus is valid and not replaced
      assertEquals("plus+", BigQueryUtils.normalizeFieldName("plus+", true));
      // colon is valid and not replaced
      assertEquals("colon:", BigQueryUtils.normalizeFieldName("colon:", true));
      // apostrophe is valid and not replaced
      assertEquals("apostrophe'", BigQueryUtils.normalizeFieldName("apostrophe'", true));
      // less than is valid and not replaced
      assertEquals("less_than<", BigQueryUtils.normalizeFieldName("less_than<", true));
      // greater than is valid and not replaced
      assertEquals("greater_than>", BigQueryUtils.normalizeFieldName("greater_than>", true));
      // number sign is valid and not replaced
      assertEquals("number_sign#", BigQueryUtils.normalizeFieldName("number_sign#", true));
      // vertical line is valid and not replaced
      assertEquals("vertical_line|", BigQueryUtils.normalizeFieldName("vertical_line|", true));

      // Testing invalid characters

      // test for tab
      assertEquals("tab_", BigQueryUtils.normalizeFieldName("tab\t", true));
      // exclamation is replaced with underscore
      assertEquals("exclamation_", BigQueryUtils.normalizeFieldName("exclamation!", true));
      // quotation is replaced with underscore
      assertEquals("quotation_", BigQueryUtils.normalizeFieldName("quotation\"", true));
      // dollar is replaced with underscore
      assertEquals("dollar_", BigQueryUtils.normalizeFieldName("dollar$", true));
      // left parenthesis is replaced with underscore
      assertEquals("left_parenthesis_", BigQueryUtils.normalizeFieldName("left_parenthesis(", true));
      // right parenthesis is replaced with underscore
      assertEquals("right_parenthesis_", BigQueryUtils.normalizeFieldName("right_parenthesis)", true));
      // asterisk is replaced with underscore
      assertEquals("asterisk_", BigQueryUtils.normalizeFieldName("asterisk*", true));
      // comma is replaced with underscore
      assertEquals("comma_", BigQueryUtils.normalizeFieldName("comma,", true));
      // period is replaced with underscore
      assertEquals("period_", BigQueryUtils.normalizeFieldName("period.", true));
      // slash is replaced with underscore
      assertEquals("slash_", BigQueryUtils.normalizeFieldName("slash/", true));
      // semicolon is replaced with underscore
      assertEquals("semicolon_", BigQueryUtils.normalizeFieldName("semicolon;", true));
      // question mark is replaced with underscore
      assertEquals("question_mark_", BigQueryUtils.normalizeFieldName("question_mark?", true));
      // at sign is replaced with underscore
      assertEquals("at_sign_", BigQueryUtils.normalizeFieldName("at_sign@", true));
      // left square bracket is replaced with underscore
      assertEquals("left_square_bracket_", BigQueryUtils.normalizeFieldName("left_square_bracket[", true));
      // backslash is replaced with underscore
      assertEquals("backslash_", BigQueryUtils.normalizeFieldName("backslash\\", true));
      // right square bracket is replaced with underscore
      assertEquals("right_square_bracket_", BigQueryUtils.normalizeFieldName("right_square_bracket]", true));
      // circumflex accent is replaced with underscore
      assertEquals("circumflex_accent_", BigQueryUtils.normalizeFieldName("circumflex_accent^", true));
      // grave accent is replaced with underscore
      assertEquals("grave_accent_", BigQueryUtils.normalizeFieldName("grave_accent`", true));
      // left curly bracket is replaced with underscore
      assertEquals("left_curly_bracket_", BigQueryUtils.normalizeFieldName("left_curly_bracket{", true));
      // right curly bracket is replaced with underscore
      assertEquals("right_curly_bracket_", BigQueryUtils.normalizeFieldName("right_curly_bracket}", true));
      // tilde is replaced with underscore
      assertEquals("tilde_", BigQueryUtils.normalizeFieldName("tilde~", true));

      // mixed valid and invalid characters
      assertEquals("mixed%valid_invalid_", BigQueryUtils.normalizeFieldName("mixed%valid?invalid@", true));

      // test for 2 space
      assertEquals("a2  fs", BigQueryUtils.normalizeFieldName("a2  fs", true));

    }

    @Test
    public void testGetMaximumExistingSequenceNumberZeroInvocations() throws Exception {
      // Zero Tables
      Set<SourceTable> allTables = generateSourceTableSet(0);
      Mockito.when(bigQueryMock.listTables(ArgumentMatchers.anyString())).thenReturn(generateBQTablesPage(0));
      long tableResult0 = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, PROJECT,
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
      Mockito.when(bigQueryMock.listTables(ArgumentMatchers.anyString())).thenReturn(generateBQTablesPage(1));
      long tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, PROJECT,
                                                                        null, bigQueryMock, null, 1000);
      assertEquals(1L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(1))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

      // Subtest2 : Ten Tables
      allTables = generateSourceTableSet(10);
      Mockito.when(bigQueryMock.listTables(ArgumentMatchers.anyString())).thenReturn(generateBQTablesPage(10));
      tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, PROJECT,
                                                                   null, bigQueryMock, null, 1000);
      assertEquals(2L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(2))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

      // Subtest3 : 1000 Tables
      allTables = generateSourceTableSet(1000);
      Mockito.when(bigQueryMock.listTables(ArgumentMatchers.anyString())).thenReturn(generateBQTablesPage(1000));
      tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, PROJECT,
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
      Mockito.when(bigQueryMock.listTables(ArgumentMatchers.anyString())).thenReturn(generateBQTablesPage(1001));
      long tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, PROJECT,
                                                                        null, bigQueryMock, null, 1000);
      assertEquals(2L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(2))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

      //Subtest2 :  2000 Tables : Should call bigquery 2 times. 1000+1000
      allTables = generateSourceTableSet(2000);
      Mockito.when(bigQueryMock.listTables(ArgumentMatchers.anyString())).thenReturn(generateBQTablesPage(2000));
      tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, PROJECT,
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
      Mockito.when(bigQueryMock.listTables(ArgumentMatchers.anyString())).thenReturn(generateBQTablesPage(2500));
      long tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, PROJECT,
                                                                        null, bigQueryMock, null, 1000);
      assertEquals(3L, tableResult);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(3))
        .invoke("executeAggregateQuery",
                ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

    }

    @Test
    public void testGetMaximumExistingSequenceNumberEmptyDatasetName() throws Exception {
      Set<SourceTable> allTables = generateSourceTableSet(1);
      Mockito.when(bigQueryMock.listTables(ArgumentMatchers.anyString())).thenReturn(generateBQTablesPage(1));
      long tableResult0 = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, PROJECT,
                                                                         "", bigQueryMock, null, 1000);
      assertEquals(1, tableResult0);
      PowerMockito.verifyPrivate(BigQueryUtils.class, times(1))
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

    }

    @Test
    public void testMaximumExistingSequenceNumber() throws Exception {
      generateBigQueryTestTables();

      //0 tables : no call
      Set<SourceTable> tables0 = generateSourceTableSet(0);
      long tableResult0 = BigQueryUtils.getMaximumExistingSequenceNumber(tables0, project,
                                                                         DATASET, bigQuery, null,
                                                                         MAX_BIG_QUERY_BATCH_SZE);
      assertEquals(0, tableResult0);

      //3 tables : 1 call
      Set<SourceTable> tables3 = generateSourceTableSet(3);
      long tableResult3 = BigQueryUtils.getMaximumExistingSequenceNumber(tables3, project,
                                                                         DATASET, bigQuery, null,
                                                                         MAX_BIG_QUERY_BATCH_SZE);
      assertEquals(3, tableResult3);

      //6 tables : 2 call
      Set<SourceTable> tables6 = generateSourceTableSet(6);
      long tableResult6 = BigQueryUtils.getMaximumExistingSequenceNumber(tables6, project,
                                                                         DATASET, bigQuery, null,
                                                                         MAX_BIG_QUERY_BATCH_SZE);
      assertEquals(6, tableResult6);

      //10 tables : 3 call
      Set<SourceTable> tables10 = generateSourceTableSet(10);
      long tableResult10 = BigQueryUtils.getMaximumExistingSequenceNumber(tables10, project,
                                                                          DATASET, bigQuery, null,
                                                                          MAX_BIG_QUERY_BATCH_SZE);
      assertEquals(10, tableResult10);

      cleanup();
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

    public static void cleanup() throws Exception {
      //delete dataset
      bigQuery.delete(DatasetId.of(project, DATASET), BigQuery.DatasetDeleteOption.deleteContents());
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

  private static Page<Table> generateBQTablesPage(int num) {
    Page<Table> pg = new Page<Table>() {
      @Override
      public boolean hasNextPage() {
        return false;
      }

      @Override
      public String getNextPageToken() {
        return null;
      }

      @Override
      public Page<Table> getNextPage() {
        return null;
      }

      @Override
      public Iterable<Table> iterateAll() {
        List<Table> tableList = new ArrayList<>();

        for (int i = 1; i <= num; i++) {
          // Create Table
          Table tableMock2 = Mockito.mock(Table.class);

          String tableName = TABLE_PREFIX + i;
          TableId tableId = TableId.of(PROJECT, DATASET, tableName);
          Mockito.when(tableMock2.getTableId()).thenReturn(tableId);
          tableList.add(tableMock2);
        }
        return tableList;
      }

      @Override
      public Iterable<Table> getValues() {
        return null;
      }
    };
    return pg;
  }
}
