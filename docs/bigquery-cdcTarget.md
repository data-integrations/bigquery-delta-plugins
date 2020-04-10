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

Quotas
------
BigQuery has a quota of 1,000 operations per table per day, and 100,000 operations per project per day.
Every load interval, for each target table, this plugin will perform one operation.
With the default value of 90 seconds, this results in 960 merge jobs for each target table.
This means if there are no other processes loading data into BigQuery for your project,
you are limited to replicating 104 tables per day.
If you would like to replicate more tables, or you have additional processes loading data, you will
need to increase the load interval accordingly.

See https://cloud.google.com/bigquery/quotas for more information about BigQuery quotas.

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

Limitations
-----------
Tables must have a primary key in order to be replicated.

Table rename operations are not supported. If a rename event is encountered, it will be ignored.

Table alters are partially supported. An existing non-nullable column can be altered into a nullable column.
New nullable columns can be added to an existing table. Any other type of alteration to the table schema
will fail. Changes to the primary key will not fail, but existing data will not rewritten to obey uniqueness
on the new primary key.

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

**Load Interval**: Number of seconds to wait before loading a batch of data into BigQuery. 
Note that BigQuery has a quota of 1000 load jobs per table, so it is not safe to set this to a value lower
than 90 seconds, unless you are certain that there are periods of the day where there will be no
updates made to the table.

**Staging Table Prefix**: Changes are first written to a staging table before merged to the final table.
Staging tables are named used this prefix prepended to the target table name.
