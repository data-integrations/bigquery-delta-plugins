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

import io.cdap.delta.api.DeltaPipelineId;
import org.junit.Assert;
import org.junit.Test;

import static io.cdap.delta.bigquery.BigQueryTarget.STAGING_BUCKET_PREFIX;

/**
 * Tests for BigQueryTarget.
 */
public class BigQueryTargetTest {

  @Test
  public void testStagingBucketName() {
    String expectedBucketName = "somebucket";
    DeltaPipelineId pipelineId = new DeltaPipelineId("ns", "app", 1L);
    Assert.assertEquals(expectedBucketName, BigQueryTarget.getStagingBucketName("somebucket", pipelineId));
    Assert.assertEquals(expectedBucketName, BigQueryTarget.getStagingBucketName("SomeBucket", pipelineId));
    Assert.assertEquals(expectedBucketName, BigQueryTarget.getStagingBucketName("somebucket  ", pipelineId));
    Assert.assertEquals(expectedBucketName, BigQueryTarget.getStagingBucketName("gs://somebucket", pipelineId));
    Assert.assertEquals(expectedBucketName, BigQueryTarget.getStagingBucketName(" gs://somebucket  ", pipelineId));
    Assert.assertEquals(expectedBucketName, BigQueryTarget.getStagingBucketName("gs://SomeBucket", pipelineId));
    expectedBucketName = STAGING_BUCKET_PREFIX + "-ns-app-1";
    Assert.assertEquals(expectedBucketName, BigQueryTarget.getStagingBucketName("   ", pipelineId));
    Assert.assertEquals(expectedBucketName, BigQueryTarget.getStagingBucketName(null, pipelineId));
  }
}
