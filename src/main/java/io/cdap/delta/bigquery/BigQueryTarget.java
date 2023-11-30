/*
 * Copyright © 2019 Cask Data, Inc.
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
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessor;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A BigQuery CDC Target
 */
@SuppressWarnings("unused")
@Name(BigQueryTarget.NAME)
@Plugin(type = DeltaTarget.PLUGIN_TYPE)
public class BigQueryTarget implements DeltaTarget {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTarget.class);
  public static final String NAME = "bigquery";
  public static final String STAGING_BUCKET_PREFIX = "df-rbq";
  public static final int CONFLICT = 409;
  private static final String GCS_SCHEME = "gs://";
  private static final String GCP_CMEK_KEY_NAME = "gcp.cmek.key.name";
  private static final int MAX_TABLES_PER_QUERY = 1000;
  private  static final String RATE_LIMIT_EXCEEDED_REASON = "rateLimitExceeded";
  private static final Set<Integer> RATE_LIMIT_EXCEEDED_CODES = new HashSet<>(Arrays.asList(400, 403));
  private  static final int BILLING_TIER_LIMIT_EXCEEDED_CODE = 400;
  private  static final String BILLING_TIER_LIMIT_EXCEEDED_REASON = "billingTierLimitExceeded";
  private  static final int RETRY_COUNT = 25;
  private final int retryCount;
  private final Conf conf;
  public static final List<String> BIGQUERY_SCOPES = Arrays.asList("https://www.googleapis.com/auth/drive",
                                                                   "https://www.googleapis.com/auth/bigquery");


  @SuppressWarnings("unused")
  public BigQueryTarget(Conf conf) {
    this(conf, RETRY_COUNT);
  }
  @SuppressWarnings("unused")
  public BigQueryTarget(Conf conf, int retryCount) {
    this.conf = conf;
    this.retryCount = retryCount;
  }

  @Override
  public void configure(Configurer configurer) {
    // no-op
  }

  @Override
  public void initialize(DeltaTargetContext context) throws Exception {

    Credentials credentials = conf.getCredentials();

    String project = conf.getDatasetProject();

    String cmekKey = context.getRuntimeArguments().get(GCP_CMEK_KEY_NAME) != null ?
      context.getRuntimeArguments().get(GCP_CMEK_KEY_NAME) : conf.getEncryptionKeyName();

    EncryptionConfiguration encryptionConfig = cmekKey == null ? null :
      EncryptionConfiguration.newBuilder().setKmsKeyName(cmekKey).build();

    BigQuery bigQuery = getBigQuery(project, credentials);

    RetryPolicy<Object> retryPolicy = createBaseRetryPolicy()
            .handleIf(ex -> {
              if (ex.getCause() instanceof IOException) {
                return true;
              }
              if (ex instanceof BigQueryException) {
                BigQueryException t = (BigQueryException) ex;
                int code = t.getCode();
                String reason = t.getError() != null ? t.getError().getReason() : null;
                boolean isRateLimitExceeded =  RATE_LIMIT_EXCEEDED_CODES.contains(code)
                        && RATE_LIMIT_EXCEEDED_REASON.equals(reason);
                boolean isBillingTierLimitExceeded = code == BILLING_TIER_LIMIT_EXCEEDED_CODE
                        && BILLING_TIER_LIMIT_EXCEEDED_REASON.equals(reason);
                return t.isRetryable() || isRateLimitExceeded || isBillingTierLimitExceeded;
              }
              return false;
            });
    try {
      long maximumExistingSequenceNumber = Failsafe.with(retryPolicy).get(() ->
              BigQueryUtils.getMaximumExistingSequenceNumber(context.getAllTables(), project, conf.getDatasetName(),
                      bigQuery, encryptionConfig, MAX_TABLES_PER_QUERY));
      LOG.info("Found maximum sequence number {}", maximumExistingSequenceNumber);
      context.initializeSequenceNumber(maximumExistingSequenceNumber);
    } catch (Exception e) {
      throw new RuntimeException("Failed to compute the maximum sequence number among all the target tables " +
              "selected for replication. Please make sure that if target tables exists, " +
              "they should have '_sequence_num' column in them.", e);
    }


  }

  @Override
  public EventConsumer createConsumer(DeltaTargetContext context) throws IOException {
    Credentials credentials = conf.getCredentials();
    String project = conf.getProject();
    String datasetProject = conf.getDatasetProject();

    String cmekKey = context.getRuntimeArguments().get(GCP_CMEK_KEY_NAME) != null ?
      context.getRuntimeArguments().get(GCP_CMEK_KEY_NAME) : conf.getEncryptionKeyName();

    EncryptionConfiguration encryptionConfig = cmekKey == null ? null :
      EncryptionConfiguration.newBuilder().setKmsKeyName(cmekKey).build();

    BigQuery bigQuery = getBigQuery(project, credentials);

    Storage storage = StorageOptions.newBuilder()
      .setCredentials(credentials)
      .build()
      .getService();

    String stagingBucketName = getStagingBucketName(conf.stagingBucket, context.getPipelineId());
    RetryPolicy<Object> retryPolicy = createBaseRetryPolicy()
            .handleIf(ex -> ex instanceof IOException
                          || ex.getCause() instanceof IOException
                          || (ex instanceof StorageException && ((StorageException) ex).isRetryable()));

    Bucket bucket;
    try {
      bucket = Failsafe.with(retryPolicy).get(() -> {
        Bucket b = storage.get(stagingBucketName);
        if (b == null) {
          try {
            BucketInfo.Builder builder = BucketInfo.newBuilder(stagingBucketName);
            if (cmekKey != null) {
              builder.setDefaultKmsKeyName(cmekKey);
            }
            if (conf.stagingBucketLocation != null && !conf.stagingBucketLocation.trim().isEmpty()) {
              builder.setLocation(conf.stagingBucketLocation);
            }
            b = storage.create(builder.build());
          } catch (StorageException e) {
            // It is possible that in multiple worker instances scenario
            // bucket is created by another worker instance after this worker instance
            // determined that the bucket does not exists. Ignore error if bucket already exists.
            if (e.getCode() != CONFLICT) {
              throw e;
            }
            b = storage.get(stagingBucketName);
          }
        }
        return b;
      });
    } catch (Exception e) {
      throw new RuntimeException(
              String.format("Unable to create staging bucket '%s' in project '%s'. " +
                      "Please make sure the service account has permission to create buckets, " +
                      "or create the bucket before starting the program.", stagingBucketName, project), e);
    }
    return new BigQueryEventConsumer(context, storage, bigQuery, bucket, datasetProject,
                                     conf.getLoadIntervalSeconds(), conf.getStagingTablePrefix(),
                                     conf.requiresManualDrops(), encryptionConfig, null, conf.getDatasetName(),
                                     conf.softDeletesEnabled(), conf.getAllowFlexibleColumnNaming());
  }

  @VisibleForTesting
  static String getStagingBucketName(@Nullable String providedBucketName, DeltaPipelineId pipelineId) {
    String stagingBucketName = providedBucketName == null ? null : providedBucketName.trim();
    if (stagingBucketName == null || stagingBucketName.isEmpty()) {
      stagingBucketName = stringifyPipelineId(pipelineId);
    }
    if (stagingBucketName.startsWith(GCS_SCHEME)) {
      stagingBucketName = stagingBucketName.substring(GCS_SCHEME.length());
    }
    return stagingBucketName.toLowerCase();
  }

  @Override
  public TableAssessor<StandardizedTableDetail> createTableAssessor(Configurer configurer) {
    return new BigQueryAssessor(conf.stagingTablePrefix, conf.datasetName, conf.getAllowFlexibleColumnNaming());
  }

  private static String stringifyPipelineId(DeltaPipelineId pipelineId) {
    return Joiner.on("-").join(STAGING_BUCKET_PREFIX, pipelineId.getNamespace(), pipelineId.getApp(),
                               pipelineId.getGeneration());
  }

  private <T> RetryPolicy<T> createBaseRetryPolicy() {
    RetryPolicy<T> retryPolicy = new RetryPolicy<>();
    return retryPolicy.withMaxAttempts(retryCount)
            .withMaxDuration(Duration.of(2, ChronoUnit.MINUTES))
            .withBackoff(1, 30, ChronoUnit.SECONDS)
            .withJitter(0.1);
  }

  public static BigQuery getBigQuery(String project, @Nullable Credentials credentials) {
    BigQueryOptions.Builder bigqueryBuilder = BigQueryOptions.newBuilder().setProjectId(project);
    if (credentials != null) {
      Set<String> scopes = new HashSet<>(BIGQUERY_SCOPES);

      if (credentials instanceof ServiceAccountCredentials) {
        scopes.addAll(((ServiceAccountCredentials) credentials).getScopes());
      } else if (credentials instanceof ExternalAccountCredentials) {
        Collection<String> currentScopes = ((ExternalAccountCredentials) credentials).getScopes();
        if (currentScopes != null) {
          scopes.addAll(currentScopes);
        }
      }

      if (credentials instanceof GoogleCredentials) {
        credentials = ((GoogleCredentials) credentials).createScoped(scopes);
      }
      bigqueryBuilder.setCredentials(credentials);
    }
    return bigqueryBuilder.build().getService();
  }

  /**
   * Config for BigQuery target.
   */
  @SuppressWarnings("unused")
  public static class Conf extends PluginConfig {

    public static final String AUTO_DETECT = "auto-detect";
    @Nullable
    @Description("Project of the BigQuery dataset. When running on a Dataproc cluster, this can be set to "
      + "'auto-detect', which will use the project of the cluster.")
    private String project;

    @Macro
    @Nullable
    @Description("The project the dataset belongs to. This is only required if the dataset is not " +
      "in the same project that the BigQuery job will run in. If no value is given, it will" +
      " default to the configured project ID.")
    private String datasetProject;

    @Macro
    @Nullable
    @Description("Service account key to use when interacting with GCS and BigQuery. The service account "
      + "must have permission to write to GCS and BigQuery. When running on a Google Cloud VM, this can be set to "
      + "'auto-detect', which will use the service account key on the VM. On non Google Cloud VM, you need to have an" +
      " environment variable 'GOOGLE_APPLICATION_CREDENTIALS' pointing to the service account key file to make " +
      "'auto-detect' work by using that service account key file.")
    private String serviceAccountKey;

    @Nullable
    @Description("GCS bucket to write the change events to before loading them into the BigQuery staging tables. "
      + "This bucket can be shared across multiple delta pipelines within the same CDAP instance. "
      + "The bucket must be in the same location as the BigQuery datasets that are being written to. "
      + "If the BigQuery datasets do not already exist, they will be created in the same location as the bucket."
      + "If not provided, new bucket will be created for each pipeline named as "
      + "'df-rbq-<namespace-name>-<pipeline-name>-<deployment-timestamp>'. Note that user will have to explicitly "
      + "delete the bucket once the pipeline is deleted.")
    private String stagingBucket;

    @Nullable
    @Description("The location where the BigQuery dataset and GCS staging bucket will be created. BigQuery dataset " +
      "and GCS Staging Bucket must be in the same location. This value is ignored if an existing GCS bucket is " +
      "specified, as the staging bucket and the BigQuery dataset will be created in the same location as that bucket")
    protected String stagingBucketLocation;

    @Nullable
    @Description("Changes are first written to a staging table before being merged to the final table. "
      + "By default, the staging table name is the target table prefixed by '_staging_'.")
    private String stagingTablePrefix;

    @Nullable
    @Description("Number of seconds to wait in between loading batches of changes into BigQuery.")
    private Integer loadInterval;

    @Nullable
    @Description("Whether to require manual intervention when a drop table or drop database event is encountered.")
    private Boolean requireManualDrops;

    @Nullable
    @Description("Whether to enable soft deletes. If enabled, when the delete event is received by the target, " +
      "'_is_deleted' column for the record will be set to true, otherwise record will be deleted from the BigQuery " +
      "table. This configuration is no-op for the sources which generates events out of order, in which case records " +
      "are always soft deleted from the BigQuery table.")
    private Boolean softDeletes;

    @Nullable
    @Description(
      "Optional. By default the dataset name is same as source database name. A valid name should only contain " +
        "letters, numbers, and underscores and maximum length can be 1024. Any invalid chars would be replaced with " +
        "underscore in the final dataset name and any characters exceeds length limit will be truncated.")
    private String datasetName;

    @Nullable
    @Description(
      "By default, the target table's column names mirror those of the source table. They are normalized to include " +
      "only letters, numbers, and underscores. Any invalid characters are replaced with underscores in the " +
      "final column name. If set to true, the target table's column names will be adjusted to adhere to BigQuery's " +
      "flexible column naming conventions, such as supporting international characters, spaces, and some more " +
      "special characters (check docs) with a maximum length of 300 characters. Any invalid characters will " +
      "be replaced with underscores in the final column name. Additionally, any characters exceeding the length " +
      "limit will be truncated."
    )
    private Boolean allowFlexibleColumnNaming;

    @Nullable
    @Description(
      "Optional. GCP Customer-managed encryption key (CMEK) used to encrypt the resources created by this target.")
    private String encryptionKeyName;

    @Nullable
    public String getDatasetName() {
      return datasetName;
    }

    public boolean getAllowFlexibleColumnNaming() {
       return allowFlexibleColumnNaming != null && allowFlexibleColumnNaming;
    }

    @Nullable
    public String getEncryptionKeyName() {
      return encryptionKeyName;
    }

    private String getStagingTablePrefix() {
      return stagingTablePrefix == null || stagingTablePrefix.isEmpty() ? "_staging_" : stagingTablePrefix;
    }

    int getLoadIntervalSeconds() {
      return loadInterval == null ? 90 : loadInterval;
    }

    public boolean requiresManualDrops() {
      return requireManualDrops == null ? false : requireManualDrops;
    }

    public boolean softDeletesEnabled() {
      return softDeletes == null ? false : softDeletes;
    }

    private String getProject() {
      if (project == null || "auto-detect".equalsIgnoreCase(project)) {
        return ServiceOptions.getDefaultProjectId();
      }
      return project;
    }

    private Credentials getCredentials() throws IOException {
      if (serviceAccountKey == null || "auto-detect".equalsIgnoreCase(serviceAccountKey)) {
        return GoogleCredentials.getApplicationDefault()
          .createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
      }

      try (InputStream is = new ByteArrayInputStream(serviceAccountKey.getBytes(StandardCharsets.UTF_8))) {
        return GoogleCredentials.fromStream(is)
          .createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
      }
    }
    public String getDatasetProject() {
      // if it is set to 'auto-detect,' the default project ID will be automatically detected
      // otherwise if the user provides an ID, it will be used.
      // or else IllegalArgument exception will be thrown
      if (AUTO_DETECT.equalsIgnoreCase(datasetProject)) {
        String defaultProject = ServiceOptions.getDefaultProjectId();
        if (defaultProject == null) {
          throw new IllegalArgumentException(
            "Could not detect Google Cloud project id from the environment. Please specify a dataset project id.");
        }
        return defaultProject;
      }
      // if it's null or empty that means it should be same as project
      return Strings.isNullOrEmpty(datasetProject) ? getProject() : datasetProject;
    }
  }
}
