package io.cdap.delta.bigquery.event;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Bucket;
import io.cdap.delta.api.DeltaTargetContext;

import java.util.List;
import java.util.Map;

public class EventHandlerConfig {

   public final DeltaTargetContext context;
   public final BigQuery bigQuery;
   public final String project;
   public final Bucket bucket;
   public final Map<TableId, List<String>> primaryKeyStore;
   public final boolean requireManualDrops;
   public final int maxClusteringColumns;
   public final EncryptionConfiguration encryptionConfig;

   public EventHandlerConfig(
           DeltaTargetContext context,
           BigQuery bigQuery,
           String project,
           Bucket bucket,
           Map<TableId,
           List<String>> primaryKeyStore,
           boolean requireManualDrops,
           int maxClusteringColumns,
           EncryptionConfiguration encryptionConfig

   ) {
      this.context = context;
      this.bigQuery = bigQuery;
      this.project = project;
      this.bucket = bucket;
      this.primaryKeyStore = primaryKeyStore;
      this.requireManualDrops = requireManualDrops;
      this.maxClusteringColumns = maxClusteringColumns;
      this.encryptionConfig = encryptionConfig;
   }
}
