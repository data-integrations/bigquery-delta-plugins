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
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.SourceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Utility class for executing queries on BigQuery.
 */
public final class BigQueryUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryUtils.class);
  public static final int FIELD_NAME_MAX_LENGTH = 300;
  static final String BACKTICK = "`";
  private static final int DATASET_OR_TABLE_NAME_MAX_LENGTH = 1024;
  // Valid BigQuery dataset names can contain only letters, numbers, and underscores.
  // See here: https://cloud.google.com/bigquery/docs/datasets#dataset-naming
  private static final Pattern VALID_DATASET_NAME_REGEX = Pattern.compile("[\\w]+");
  private static final Pattern INVALID_DATASET_NAME_REGEX = Pattern.compile("[^\\w]+");
  // Valid BigQuery table names can contain only Unicode characters in category L (letter), M (mark), N (number),
  // Pc (connector, including underscore), Pd (dash), Zs (space).
  // See here: https://cloud.google.com/bigquery/docs/tables#table_naming
  private static final Pattern VALID_TABLE_NAME_REGEX = Pattern.compile("[\\p{L}\\p{M}\\p{N}\\p{Pc}\\p{Pd}\\p{Zs}]+");
  private static final Pattern INVALID_TABLE_NAME_REGEX =
          Pattern.compile("[^\\p{L}\\p{M}\\p{N}\\p{Pc}\\p{Pd}\\p{Zs}]+");
  private static final Pattern VALID_FIELD_NAME_REGEX =
          Pattern.compile("[\\p{L}\\p{M}\\p{N}\\p{Pc}\\p{Pd}&%+=:'<>#| ]+");
  private static final Pattern INVALID_FIELD_NAME_REGEX =
          Pattern.compile("[^\\p{L}\\p{M}\\p{N}\\p{Pc}\\p{Pd}&%+=:'<>#| ]+");
  private static final String BIG_QUERY_DUPLICATE_ERROR = "duplicate";

  private static final Set<String> BQ_ABORT_REASONS = new HashSet<>(Arrays.asList("invalid", "invalidQuery"));
  private static final int BQ_INVALID_REQUEST_CODE = 400;

  private BigQueryUtils() {
  }

  static long getMaximumExistingSequenceNumber(Set<SourceTable> allTables, String project,
                                               @Nullable String datasetName, BigQuery bigQuery,
                                               EncryptionConfiguration encryptionConfiguration,
                                               int maxTablesPerQuery) throws InterruptedException {

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
                        EncryptionConfiguration encryptionConfiguration) throws InterruptedException {
    SourceTable table0 = allTables.stream().findFirst().get();
    Set<TableId> existingTableIDs = new HashSet<>();
    String dataset = getNormalizedDatasetName(datasetName, table0.getDatabase());
    if (bigQuery.getDataset(dataset) != null) {
      for (Table table : bigQuery.listTables(dataset).iterateAll()) {
        existingTableIDs.add(table.getTableId());
      }
    }

    StringBuilder builder = new StringBuilder();
    builder.append("SELECT MAX(max_sequence_num) FROM (");
    List<String> maxSequenceNumQueryPerTable = new ArrayList<>();
    for (SourceTable table : allTables) {
      TableId tableId = TableId.of(project, getNormalizedDatasetName(datasetName, table.getDatabase()),
                                   normalizeTableName(table.getTable()));
      if (existingTableIDs.contains(tableId)) {
        maxSequenceNumQueryPerTable.add(String.format("SELECT MAX(_sequence_num) as max_sequence_num FROM %s",
                                  wrapInBackTick(tableId.getProject(), tableId.getDataset(), tableId.getTable())));
      }
    }

    builder.append(String.join(" UNION ALL ", maxSequenceNumQueryPerTable));
    builder.append(");");

    long maxSequenceNumber = maxSequenceNumQueryPerTable.size() == 0 ? 0 : executeAggregateQuery(bigQuery,
            builder.toString(),
            encryptionConfiguration);;
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
            wrapInBackTick(tableId.getProject(), tableId.getDataset(), tableId.getTable()));
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
   * Get normalized dataset name for target dataset
   * Method returns normalized datasetName if it is not empty
   * Otherwise it returns normalized form of databaseName
   * @param datasetName dataset name to be normalized
   * @param databaseName database name to be used if datasetName is empty
   * @return normalized datasetName if it is not empty otherwise normalized form of databaseName
   */
  public static String getNormalizedDatasetName(@Nullable String datasetName, String databaseName) {
    if (Strings.isNullOrEmpty(datasetName)) {
      return normalizeDatasetName(databaseName);
    }
    return normalizeDatasetName(datasetName);
  }

  /**
   * Normalize the dataset name according to BigQuery's requirement.
   * The name must contain only letters, numbers, and underscores.
   * And it must be 1024 characters or fewer.
   * @param name the dataset name to be normalized
   * @return the normalized name
   */
  public static String normalizeDatasetName(String name) {
    return normalize(name, DATASET_OR_TABLE_NAME_MAX_LENGTH, true, false, false);
  }

  /**
   * Normalize the table name according to BigQuery's requirement.
   * The name must contain only Unicode characters in category L (letter), M (mark), N (number),
   * Pc (connector, including underscore), Pd (dash), Zs (space).
   * See here: https://cloud.google.com/bigquery/docs/tables#table_naming
   * @param name the dataset name to be normalized
   * @return the normalized name
   */
  public static String normalizeTableName(String name) {
    return normalize(name, DATASET_OR_TABLE_NAME_MAX_LENGTH, true, true, false);
  }

  /**
   * Normalize the field name according to BigQuery's requirement.
   * The name must contain only letters, numbers, and underscores, start with a letter or underscore.
   * See here: https://cloud.google.com/bigquery/docs/schemas#flexible-column-names
   * And it must be 300 characters or fewer.
   * @param name the field name to be normalized
   * @return the normalized name
   */
  public static String normalizeFieldName(String name, boolean allowFlexibleColumnNaming) {
    // use extended charset is set false due to backward compatibility
    // use allowFlexibleColumnNaming to determine whether to use extended charset
    return normalize(name, FIELD_NAME_MAX_LENGTH, false, false, allowFlexibleColumnNaming);
  }

  private static String normalize(String name, int maxLength, boolean canStartWithNumber, boolean useExtendedCharset,
                                  boolean allowFlexibleColumnNames) {
    if (name == null || name.isEmpty()) {
      return name;
    }
    // replace invalid chars with underscores if there are any
    if (allowFlexibleColumnNames && !VALID_FIELD_NAME_REGEX.matcher(name).matches()) {
      name = INVALID_FIELD_NAME_REGEX.matcher(name).replaceAll("_");
    }
    if (useExtendedCharset && !VALID_TABLE_NAME_REGEX.matcher(name).matches() && !allowFlexibleColumnNames) {
      name = INVALID_TABLE_NAME_REGEX.matcher(name).replaceAll("_");
    }
    if (!useExtendedCharset && !VALID_DATASET_NAME_REGEX.matcher(name).matches() && !allowFlexibleColumnNames) {
      name = INVALID_DATASET_NAME_REGEX.matcher(name).replaceAll("_");
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

  public static DMLEvent.Builder normalize(DMLEvent event, SchemaMappingCache schemaMappingCache,
                                           boolean allowFlexibleColumnNaming) {

    DMLEvent.Builder normalizedEventBuilder = DMLEvent.builder(event);
    if (event.getRow() != null) {
      normalizedEventBuilder.setRow(normalize(event.getOperation(), event.getRow(), schemaMappingCache,
              allowFlexibleColumnNaming));
    }
    if (event.getPreviousRow() != null) {
      normalizedEventBuilder.setPreviousRow(normalize(event.getOperation(), event.getPreviousRow(),
                                                      schemaMappingCache, allowFlexibleColumnNaming));
    }
    return normalizedEventBuilder;
  }

  private static StructuredRecord normalize(DMLOperation operation, StructuredRecord record,
                                            SchemaMappingCache schemaMappingCache, boolean allowFlexibleColumnNaming) {
    Schema schema = record.getSchema();
    SchemaMappingCache.SchemaMapping schemaMapping = getSchemaMapping(operation.getTableName(),
            schemaMappingCache, schema, allowFlexibleColumnNaming);

    Schema bqSchema = schemaMapping.getMappedSchema();
    Map<String, String> fieldNameMapping = schemaMapping.getFieldNameMapping();

    StructuredRecord.Builder builder = StructuredRecord.builder(bqSchema);
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      String normalizedFieldName = fieldNameMapping.get(fieldName);
      builder.set(normalizedFieldName, record.get(fieldName));
    }
    return builder.build();
  }

  private static SchemaMappingCache.SchemaMapping getSchemaMapping(String table,
                                                                   SchemaMappingCache schemaMappingCache,
                                                                   Schema schema, boolean allowFlexibleColumnNaming) {
    SchemaMappingCache.SchemaMapping schemaMapping = schemaMappingCache.get(schema);
    if (schemaMapping == null) {
      // This should be infrequent as the mapping should be fetched from cache
      // unless there are schema changes
      LOG.info("Mapping CDAP schema to BigQuery schema for table {}", table);
      schemaMapping = createSchemaMapping(schema, allowFlexibleColumnNaming);
      schemaMappingCache.put(schema, schemaMapping);
    }
    return schemaMapping;
  }

  private static SchemaMappingCache.SchemaMapping createSchemaMapping(Schema schema,
                                                                      boolean allowFlexibleColumnNaming) {
    List<Schema.Field> fields = schema.getFields();
    List<Schema.Field> normalizedFields = new ArrayList<>(fields.size());
    Map<String, String> fieldNameMapping = new HashMap<>();
    for (Schema.Field field : fields) {
      String normalizedName = normalizeFieldName(field.getName(), allowFlexibleColumnNaming);
      normalizedFields.add(Schema.Field.of(normalizedName, field.getSchema()));
      fieldNameMapping.put(field.getName(), normalizedName);
    }
    Schema bqSchema = Schema.recordOf(schema.getRecordName(), normalizedFields);
    return new SchemaMappingCache.SchemaMapping(bqSchema, fieldNameMapping);
  }

  static String wrapInBackTick(String datasetName, String tableName) {
    return BACKTICK + datasetName + "." + tableName + BACKTICK;
  }

  static String wrapInBackTick(String project, String datasetName, String tableName) {

    return BACKTICK + project + "." + datasetName + "." + tableName + BACKTICK;
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

  /**
   * Checks if BigQuery exception is due to invalid request
   *
   * @param ex {@link BigQueryException}
   * @return true if BigQuery exception is due to invalid request
   */
  public static boolean isInvalidOperationError(BigQueryException ex) {
    if (ex.getCode() == BQ_INVALID_REQUEST_CODE && ex.getError() != null) {
      BigQueryError error = ex.getError();
      if (BQ_ABORT_REASONS.contains(error.getReason())) {
        return true;
      }
    }
    return false;
  }
}
