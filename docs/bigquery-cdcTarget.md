# Google BigQuery Delta Target

Description
-----------
This CDC Target plugin writes to BigQuery. The plugin requires write access to both BigQuery and
a GCS staging bucket. Change events are first written in batches to GCS. They are then loaded into
staging tables in BigQuery. Finally, changes from the staging table are merged into the final target
table using a BigQuery merge query.

The final target tables will include all the original columns from the source table plus one additional
_sequence_num column. The sequence number is used to ensure that data is not duplicated or missed in
replicator failure scenarios.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access BigQuery.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------

**Project**: Project of the BigQuery dataset. When running on a Dataproc cluster, this can be left blank,
which will use the project of the cluster.

**Staging Bucket**: GCS bucket to write change events to before loading them into staging tables.
This bucket can be shared across multiple replicators. The bucket must be in the same location as the
BigQuery dataset.

**Service Account Key**: The contents of the service account key to use when interacting with GCS and
BigQuery. When running on a Dataproc cluster, this can be left blank, which will use the service account
of the cluster.

**Max Batch Seconds**: Maximum number of seconds to wait before merging a batch of changes.

**Max Batch Changes**: Maximum number of change events to include in a single batch.

**Staging Table Prefix**: Changes are first written to a staging table before merged to the final table.
Staging tables are named used this prefix prepended to the target table name.
