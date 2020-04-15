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
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaTarget;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * A BigQuery CDC Target
 */
@SuppressWarnings("unused")
@Name(BigQueryTarget.NAME)
@Plugin(type = DeltaTarget.PLUGIN_TYPE)
public class BigQueryTarget implements DeltaTarget {
  public static final String NAME = "bigquery";
  private final Conf conf;

  @SuppressWarnings("unused")
  public BigQueryTarget(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configure(Configurer configurer) {
    // no-op
  }

  @Override
  public EventConsumer createConsumer(DeltaTargetContext context) throws IOException {
    Credentials credentials = conf.getCredentials();
    String project = conf.getProject();
    String cmekKey = context.getRuntimeArguments().get("gcp.cmek.key.name");
    EncryptionConfiguration encryptionConfig = cmekKey == null ? null :
      EncryptionConfiguration.newBuilder().setKmsKeyName(cmekKey).build();

    BigQuery bigQuery = BigQueryOptions.newBuilder()
      .setCredentials(credentials)
      .setProjectId(project)
      .build()
      .getService();

    Storage storage = StorageOptions.newBuilder()
      .setCredentials(credentials)
      .setProjectId(project)
      .build()
      .getService();
    // TODO: make bucket optional
    Bucket bucket = storage.get(conf.stagingBucket);
    if (bucket == null) {
      try {
        // TODO: make bucket location configurable
        BucketInfo.Builder builder = BucketInfo.newBuilder(conf.stagingBucket);
        if (cmekKey != null) {
          builder.setDefaultKmsKeyName(cmekKey);
        }
        bucket = storage.create(builder.build());
      } catch (StorageException e) {
        throw new IOException(
          String.format("Unable to create staging bucket '%s' in project '%s'. "
                          + "Please make sure the service account has permission to create buckets, "
                          + "or create the bucket before starting the program.", conf.stagingBucket, project), e);
      }
    }

    return new BigQueryEventConsumer(context, storage, bigQuery, bucket, project,
                                     conf.getLoadIntervalSeconds(), conf.getStagingTablePrefix(),
                                     conf.requiresManualDrops(), encryptionConfig);
  }

  @Override
  public TableAssessor<StandardizedTableDetail> createTableAssessor(Configurer configurer) {
    return new BigQueryAssessor(conf.stagingTablePrefix, conf.loadInterval);
  }

  /**
   * Config for BigQuery target.
   */
  @SuppressWarnings("unused")
  public static class Conf extends PluginConfig {

    @Nullable
    @Description("Project of the BigQuery dataset. When running on a Google Cloud VM, this can be set to "
      + "'auto-detect', which will use the project of the VM.")
    private String project;

    @Macro
    @Nullable
    @Description("Service account key to use when interacting with GCS and BigQuery. The service account "
      + "must have permission to write to GCS and BigQuery. When running on a Google Cloud VM, this can be set to "
      + "'auto-detect', which will use the service account key on the VM.")
    private String serviceAccountKey;

    @Description("GCS bucket to write the change events to before loading them into the staging tables. "
      + "This bucket can be shared across multiple delta pipelines within the same CDAP instance. "
      + "The bucket must be in the same location as the BigQuery datasets that are being written to. "
      + "If the BigQuery datasets do not already exist, they will be created in the same location as the bucket.")
    private String stagingBucket;

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

    private String getStagingTablePrefix() {
      return stagingTablePrefix == null || stagingTablePrefix.isEmpty() ? "_staging_" : stagingTablePrefix;
    }

    int getLoadIntervalSeconds() {
      return loadInterval == null ? 90 : loadInterval;
    }

    public boolean requiresManualDrops() {
      return requireManualDrops == null ? false : requireManualDrops;
    }

    private String getProject() {
      if (project == null || "auto-detect".equalsIgnoreCase(project)) {
        return ServiceOptions.getDefaultProjectId();
      }
      return project;
    }

    private Credentials getCredentials() throws IOException {
      if (serviceAccountKey == null || "auto-detect".equalsIgnoreCase(serviceAccountKey)) {
        return GoogleCredentials.getApplicationDefault();
      }

      try (InputStream is = new ByteArrayInputStream(serviceAccountKey.getBytes(StandardCharsets.UTF_8))) {
        return GoogleCredentials.fromStream(is)
          .createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
      }
    }
  }
}
