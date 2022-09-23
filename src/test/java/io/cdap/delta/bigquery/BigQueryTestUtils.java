/*
 * Copyright © 2022 Cask Data, Inc.
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
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;

import java.util.UUID;

public class BigQueryTestUtils {

    public static TableResult executeQuery(String query, BigQuery bigQuery) throws InterruptedException {
        QueryJobConfiguration jobConfig = QueryJobConfiguration.of(query);
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(jobConfig).setJobId(jobId).build());
        queryJob.waitFor();
        return queryJob.getQueryResults();
    }

    public static void cleanupTest(Bucket bucket, String dataset, BigQueryEventConsumer eventConsumer,
                                   BigQuery bigQuery, Storage storage) {
        for (Blob blob : bucket.list().iterateAll()) {
            storage.delete(blob.getBlobId());
        }
        bucket.delete();
        Dataset ds = bigQuery.getDataset(dataset);
        if (ds != null) {
            ds.delete(BigQuery.DatasetDeleteOption.deleteContents());
        }
        eventConsumer.stop();
    }
}
