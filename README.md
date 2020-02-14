# bigquery-delta-plugins
BigQuery Delta Replicator Plugins

Tests require an actual connection to GCP. To run the tests, set the following system properties:

  -Dproject.id=[gcp project id] -Dservice.account.file=[path to service account file]
  
The service account will need permission to create GCS buckets, write and delete blobs in GCS,
create and delete BigQuery datasets and table, and write/read for BigQuery tables.
