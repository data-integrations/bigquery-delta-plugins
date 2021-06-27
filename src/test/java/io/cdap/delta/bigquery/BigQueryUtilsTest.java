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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.common.base.Strings;
import io.cdap.delta.api.SourceTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;

@PrepareForTest(BigQueryUtils.class)
@RunWith(PowerMockRunner.class)
public class BigQueryUtilsTest {

  BigQuery bigQueryMock;
  Table tableMock;

  @Before
  public void init() throws Exception {
    //Mocks
    bigQueryMock = Mockito.mock(BigQuery.class);
    tableMock = Mockito.mock(Table.class);
    Mockito.when(bigQueryMock.getTable(ArgumentMatchers.any())).thenReturn(tableMock);
    PowerMockito.spy(BigQueryUtils.class);

    //Stubs
    PowerMockito.doReturn(1L,2L,3L,4L)
      .when(BigQueryUtils.class, "executeAggregateQuery",
            ArgumentMatchers.eq(bigQueryMock),ArgumentMatchers.any(),ArgumentMatchers.any());
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
    long tableResult0 = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, "testproject",
                                                                       null, bigQueryMock, null);
    assertEquals(0L, tableResult0);
    PowerMockito.verifyPrivate(BigQueryUtils.class, times(0))
      .invoke("executeAggregateQuery", ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

  }

  @Test
  public void testGetMaximumExistingSequenceNumberSingleInvocations() throws Exception {

    // Subtest : One Table
    Set<SourceTable> allTables = generateSourceTableSet(1);
    long tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, "testproject",
                                                                      null, bigQueryMock, null);
    assertEquals(1L, tableResult);
    PowerMockito.verifyPrivate(BigQueryUtils.class, times(1))
      .invoke("executeAggregateQuery", ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

    // Subtest2 : Ten Tables
    allTables = generateSourceTableSet(10);
    tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, "testproject",
                                                                 null, bigQueryMock, null);
    assertEquals(2L, tableResult);
    PowerMockito.verifyPrivate(BigQueryUtils.class, times(2))
      .invoke("executeAggregateQuery", ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

    // Subtest3 : 1000 Tables
    allTables = generateSourceTableSet(1000);
    tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, "testproject",
                                                                 null, bigQueryMock, null);
    assertEquals(3L, tableResult);
    PowerMockito.verifyPrivate(BigQueryUtils.class, times(3))
      .invoke("executeAggregateQuery", ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

  }

  @Test
  public void testGetMaximumExistingSequenceNumberDoubleInvocations() throws Exception {

    //Subtest1 :  1001 Tables : Should call bigquery 2 times. 1000+1
    Set<SourceTable> allTables = generateSourceTableSet(1001);
    long tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, "testproject",
                                                                      null, bigQueryMock, null);
    assertEquals(2L, tableResult);
    PowerMockito.verifyPrivate(BigQueryUtils.class, times(2))
      .invoke("executeAggregateQuery", ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

    //Subtest2 :  2000 Tables : Should call bigquery 2 times. 1000+1000
    allTables = generateSourceTableSet(2000);
    tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, "testproject",
                                                                 null, bigQueryMock, null);
    assertEquals(4L, tableResult);
    PowerMockito.verifyPrivate(BigQueryUtils.class, times(4))
      .invoke("executeAggregateQuery", ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

  }

  @Test
  public void testGetMaximumExistingSequenceNumberTripleInvocations() throws Exception {

    //Subtest1 :  2500 Tables : Should call bigquery 3 times. 1000+1000+500
    Set<SourceTable> allTables = generateSourceTableSet(2500);
    long tableResult = BigQueryUtils.getMaximumExistingSequenceNumber(allTables, "testproject",
                                                                      null, bigQueryMock, null);
    assertEquals(3L, tableResult);
    PowerMockito.verifyPrivate(BigQueryUtils.class, times(3))
      .invoke("executeAggregateQuery", ArgumentMatchers.eq(bigQueryMock), ArgumentMatchers.any(), ArgumentMatchers.any());

  }


  // Helpers Functions :
  private Set<SourceTable> generateSourceTableSet(int noOfTables) {
    Set<SourceTable> allTables = new HashSet<>();
    for (int i = 0; i < noOfTables; i++) {
      allTables.add(new SourceTable("testdatabase", "testtable_" + (i + 1),
                                    null, null, new HashSet<>(), new HashSet<>()));
    }
    return allTables;
  }

}
