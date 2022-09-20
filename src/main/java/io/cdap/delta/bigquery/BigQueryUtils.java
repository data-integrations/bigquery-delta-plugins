/*
 * Copyright © 2020 Cask Data, Inc.
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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.SourceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Utility class for executing queries on BigQuery.
 */
public final class BigQueryUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryUtils.class);
  public static final int FIELD_NAME_MAX_LENGTH = 128;
  static final String BACKTICK = "`";
  private static final int DATASET_OR_TABLE_NAME_MAX_LENGTH = 1024;
  // according to big query dataset and table naming convention, valid name should only contain letters (upper or
  // lower case), numbers, and underscores
  private static final String VALID_NAME_REGEX = "[\\w]+";
  private static final String INVALID_NAME_REGEX = "[^\\w]+";
  private static final String BIG_QUERY_DUPLICATE_ERROR = "duplicate";

  private BigQueryUtils() {
  }

  static long getMaximumExistingSequenceNumber(Set<SourceTable> allTables, String project,
                                               @Nullable String datasetName, BigQuery bigQuery,
                                               EncryptionConfiguration encryptionConfiguration,
                                               int maxTablesPerQuery) {

    Set<SourceTable> allRemainingTables = allTables;
    long maxExisting = 0;
    while (allRemainingTables.size() > 0) {
      // select <maxTablesPerQuery> tables from all tables
      HashSet<SourceTable> currentBatch = Sets.newHashSet(Iterables.limit(allRemainingTables, maxTablesPerQuery));
      // find max among those <maxTablesPerQuery> tables
      long currentMax = BigQueryUtils.getMaximumExistingSequenceNumberPerBatch(currentBatch, project,
                                                                       datasetName, bigQuery,
                                                                       encryptionConfiguration);
      maxExisting = Math.max(maxExisting, currentMax);
      // remove current batch of tables from all tables.
      allRemainingTables = Sets.difference(allRemainingTables, currentBatch);
    }

    return maxExisting;
  }

  /**
   * Get the maximum existing sequence number from all tables in the target dataset which are selected for replication.
   * If those tables do not exists '0' is returned. If the target table exists but query fails possibly because of the
   * missing '_sequence_num' column in any of the tables, exception will be thrown.
   */
  static long getMaximumExistingSequenceNumberPerBatch(Set<SourceTable> allTables, String project,
                                                       @Nullable String datasetName, BigQuery bigQuery,
                                                       EncryptionConfiguration encryptionConfiguration) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT MAX(max_sequence_num) FROM (");
    List<String> maxSequenceNumQueryPerTable = new ArrayList<>();
    for (SourceTable table : allTables) {
      TableId tableId = TableId.of(project, datasetName != null ? normalizeDatasetOrTableName(datasetName) :
        normalizeDatasetOrTableName(table.getDatabase()), normalizeDatasetOrTableName(table.getTable()));
      if (bigQuery.getTable(tableId) != null) {
        maxSequenceNumQueryPerTable.add(String.format("SELECT MAX(_sequence_num) as max_sequence_num FROM %s",
                                                      wrapInBackTick(tableId.getDataset(), tableId.getTable())));
      }
    }

    builder.append(String.join(" UNION ALL ", maxSequenceNumQueryPerTable));
    builder.append(");");

    long maxSequenceNumber;
    try {
      maxSequenceNumber = maxSequenceNumQueryPerTable.size() == 0 ? 0 : executeAggregateQuery(bigQuery,
                                                                                              builder.toString(),
                                                                                              encryptionConfiguration);
    } catch (Exception e) {
      throw new RuntimeException("Failed to compute the maximum sequence number among all the target tables " +
                                   "selected for replication. Please make sure that if target tables exists, " +
                                   "they should have '_sequence_num' column in them.", e);
    }
    return maxSequenceNumber;
  }

  /**
   * Get the maximum sequence number from a specified table.
   */
  static long getMaximumSequenceNumberForTable(BigQuery bigQuery, TableId tableId,
                                               EncryptionConfiguration encryptionConfig) throws Exception {
    if (bigQuery.getTable(tableId) == null) {
      return 0L;
    }

    String query = String.format("SELECT MAX(_sequence_num) FROM %s",
                                 wrapInBackTick(tableId.getDataset(), tableId.getTable()));
    return executeAggregateQuery(bigQuery, query, encryptionConfig);
  }

  private static long executeAggregateQuery(BigQuery bigQuery, String query, EncryptionConfiguration encryptionConfig)
    throws InterruptedException {
    QueryJobConfiguration.Builder jobConfigBuilder = QueryJobConfiguration.newBuilder(query);
    if (encryptionConfig != null) {
      jobConfigBuilder.setDestinationEncryptionConfiguration(encryptionConfig);
    }
    QueryJobConfiguration jobConfig = jobConfigBuilder.build();
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = createBigQueryJob(bigQuery, JobInfo.newBuilder(jobConfig).setJobId(jobId).build());
    queryJob.waitFor();
    TableResult result = queryJob.getQueryResults();
    Iterator<FieldValueList> resultIter = result.iterateAll().iterator();
    if (!resultIter.hasNext()) {
      return 0L;
    }

    // the query passed in is expected to generate an aggregated result which contains one row and one column.
    FieldValue val = resultIter.next().get(0);
    if (val.getValue() == null) {
      return 0L;
    }

    return val.getLongValue();
  }

  /**
   * Normalize the dataset or table name according to BigQuery's requirement.
   * The name  must contain only letters, numbers, and underscores.
   * And it must be 1024 characters or fewer.
   * @param name the dataset name or table name to be normalized
   * @return the normalized name
   */
  public static String normalizeDatasetOrTableName(String name) {
    return normalize(name, DATASET_OR_TABLE_NAME_MAX_LENGTH, true);
  }

  /**
   * Normalize the field name according to BigQuery's requirement.
   * The name must contain only letters, numbers, and underscores, start with a letter or underscore.
   * And it must be 128 characters or fewer.
   * @param name the field name to be normalized
   * @return the normalized name
   */
  public static String normalizeFieldName(String name) {
    return normalize(name, FIELD_NAME_MAX_LENGTH, false);
  }

  private static String normalize(String name, int maxLength, boolean canStartWithNumber) {
    if (name == null || name.isEmpty()) {
      return name;
    }

    // replace invalid chars with underscores if there are any
    if (!name.matches(VALID_NAME_REGEX)) {
      name = name.replaceAll(INVALID_NAME_REGEX, "_");
    }

    // prepend underscore if the first character is a number and the name cannot start with number
    if (!canStartWithNumber) {
      char first = name.charAt(0);
      if (first >= '0' && first <= '9') {
        name = "_" + name;
      }
    }

    // truncate the name if it exceeds the max length
    if (name.length() > maxLength) {
      name = name.substring(0, maxLength);
    }


    return name;
  }

  public static DMLEvent.Builder normalize(DMLEvent event) {

    DMLEvent.Builder normalizedEventBuilder = DMLEvent.builder(event);
    if (event.getRow() != null) {
      normalizedEventBuilder.setRow(normalize(event.getRow()));
    }
    if (event.getPreviousRow() != null) {
      normalizedEventBuilder.setPreviousRow(normalize(event.getRow()));
    }
    return normalizedEventBuilder;
  }

  private static StructuredRecord normalize(StructuredRecord record) {
    Schema schema = record.getSchema();
    List<Schema.Field> fields = schema.getFields();
    List<Schema.Field> normalizedFields = new ArrayList<>(fields.size());
    Map<String, Object> valueMap = new HashMap<>();
    for (Schema.Field field : fields) {
      String normalizedName = normalizeFieldName(field.getName());
      normalizedFields.add(Schema.Field.of(normalizedName, field.getSchema()));
      valueMap.put(normalizedName, record.get(field.getName()));
    }
    StructuredRecord.Builder builder =
      StructuredRecord.builder(Schema.recordOf(schema.getRecordName(), normalizedFields));
    for (Schema.Field normalizedField : normalizedFields) {
      builder.set(normalizedField.getName(), valueMap.get(normalizedField.getName()));
    }
    return builder.build();
  }

  static String wrapInBackTick(String datasetName, String tableName) {
    return BACKTICK + datasetName + "." + tableName + BACKTICK;
  }

  /**
   * Tries to submit a BQ job. If there is an Already Exists Exception, it will fetch the existing job
   * @param bigquery
   * @param jobInfo
   * @return BQ Job
   */
  public static Job createBigQueryJob(BigQuery bigquery, JobInfo jobInfo) {
    try {
      return bigquery.create(jobInfo);
    } catch (BigQueryException e) {
      if (e.getCode() == 409 && BIG_QUERY_DUPLICATE_ERROR.equalsIgnoreCase(e.getReason())) {
        LOG.warn("Got JOB ALREADY EXISTS for the job id : " + jobInfo.getJobId() + ". Returning existing job");
        return bigquery.getJob(jobInfo.getJobId());
      }
      throw e;
    }
  }
}
