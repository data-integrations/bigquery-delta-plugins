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

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaTargetContext;
import net.jodah.failsafe.FailsafeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;




import static io.cdap.delta.bigquery.BigQueryTarget.STAGING_BUCKET_PREFIX;

/**
 * Tests for BigQueryTarget.
 */
@PrepareForTest({BigQueryUtils.class, BigQueryTarget.class, StorageOptions.class, BucketInfo.class})
@RunWith(PowerMockRunner.class)
public class BigQueryTargetTest {

  private static final String GCP_CMEK_KEY_NAME = "gcp.cmek.key.name";
  private static final String PROJECT = "project";
  private  static final String RATE_LIMIT_EXCEEDED_REASON = "rateLimitExceeded";
  private static final Set<Integer> RATE_LIMIT_EXCEEDED_CODES = new HashSet<>(Arrays.asList(400, 403));
  private  static final int BILLING_TIER_LIMIT_EXCEEDED_CODE = 400;
  private  static final String BILLING_TIER_LIMIT_EXCEEDED_REASON = "billingTierLimitExceeded";
  private static final Integer NOT_IMPLEMENTED_CODE = 501;
  @Rule
  private final ExpectedException exceptionRule = ExpectedException.none();
  @Mock
  private DeltaTargetContext deltaTargetContext;
  @Mock
  private BigQueryTarget.Conf conf;
  @Mock
  private StorageOptions.Builder builder;
  @Mock
  private BucketInfo.Builder bucketInfoBuilder;
  @Mock
  private Storage storage;
  @Mock
  private Credentials credentials;
  @Mock
  private BucketInfo bucketInfo;
  @Mock
  StorageOptions options;
  private final DeltaPipelineId pipelineId = new DeltaPipelineId("ns", "app", 1L);

  @Before
  public void setUp() throws Exception {
    Mockito.when(deltaTargetContext.getRuntimeArguments()).thenReturn(new HashMap<String, String>() {{
      put(GCP_CMEK_KEY_NAME, "GCP_CMEK_KEY_NAME");
    }});
    PowerMockito.when(conf, "getProject").thenReturn(PROJECT);
    PowerMockito.when(conf, "getCredentials").thenReturn(credentials);
    Mockito.when(builder.setCredentials(credentials)).thenReturn(builder);
    Mockito.when(builder.setProjectId(PROJECT)).thenReturn(builder);
    PowerMockito.whenNew(StorageOptions.Builder.class).withNoArguments().thenReturn(builder);
    Mockito.when(deltaTargetContext.getPipelineId()).thenReturn(pipelineId);

  }

  @Test
  public void testStagingBucketName() {
    String expectedBucketName = "somebucket";
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

  @Test
  public void testGetMaximumExistingSequenceNumberForRetryableFailures() throws Exception {
    List<Throwable> exceptions = new ArrayList<>();
    exceptions.add(new BigQueryException(500, null));
    exceptions.add(new BigQueryException(BILLING_TIER_LIMIT_EXCEEDED_CODE, null,
            new BigQueryError(BILLING_TIER_LIMIT_EXCEEDED_REASON, null, null)));
    exceptions.add(new BigQueryException(RATE_LIMIT_EXCEEDED_CODES.stream().findAny().get(), null,
            new BigQueryError(RATE_LIMIT_EXCEEDED_REASON, null, null)));

    BigQueryTarget bqTarget = new BigQueryTarget(conf);

    for (Throwable exception: exceptions) {
      PowerMockito.mockStatic(BigQueryUtils.class);
      PowerMockito.doThrow(exception).when(BigQueryUtils.class);
      BigQueryUtils.getMaximumExistingSequenceNumber(Mockito.anySet(), Mockito.anyString(),
              Mockito.nullable(String.class), Mockito.any(BigQuery.class),
              Mockito.nullable(EncryptionConfiguration.class), Mockito.anyInt());
      try {
        exceptionRule.expect(exception.getClass());
        bqTarget.initialize(deltaTargetContext);
      } finally {
        //verify at least 1 retry happens
        PowerMockito.verifyStatic(BigQueryUtils.class, Mockito.atLeast(2));
        BigQueryUtils.getMaximumExistingSequenceNumber(Mockito.anySet(), Mockito.anyString(),
                Mockito.nullable(String.class), Mockito.any(BigQuery.class),
                Mockito.nullable(EncryptionConfiguration.class), Mockito.anyInt());
      }
    }
  }

  @Test
  public void testGetMaximumExistingSequenceNumberForNonRetryableFailures() throws Exception {
    List<Throwable> exceptions = new ArrayList<>();
    exceptions.add(new BigQueryException(NOT_IMPLEMENTED_CODE, null));
    exceptions.add(new RuntimeException());

    BigQueryTarget bqTarget = new BigQueryTarget(conf);

    for (Throwable exception: exceptions) {
      PowerMockito.mockStatic(BigQueryUtils.class);
      PowerMockito.doThrow(exception).when(BigQueryUtils.class);
      BigQueryUtils.getMaximumExistingSequenceNumber(Mockito.anySet(), Mockito.anyString(),
              Mockito.nullable(String.class), Mockito.any(BigQuery.class),
              Mockito.nullable(EncryptionConfiguration.class), Mockito.anyInt());
      try {
        exceptionRule.expect(exception.getClass());
        bqTarget.initialize(deltaTargetContext);
      } finally {
        //Verify no retries
        PowerMockito.verifyStatic(BigQueryUtils.class, Mockito.times(1));
        BigQueryUtils.getMaximumExistingSequenceNumber(Mockito.anySet(), Mockito.anyString(),
                Mockito.nullable(String.class), Mockito.any(BigQuery.class),
                Mockito.nullable(EncryptionConfiguration.class), Mockito.anyInt());
      }
    }
  }

  @Test
  public void testCreateConsumerRetryableFailureForGcsCreate() throws Exception {
    Mockito.when(options.getService()).thenReturn(storage);
    Mockito.when(builder.build()).thenReturn(options);

    PowerMockito.mockStatic(BucketInfo.class);
    PowerMockito.doReturn(bucketInfoBuilder).when(BucketInfo.class);
    BucketInfo.newBuilder(Mockito.nullable(String.class));
    Mockito.when(bucketInfoBuilder.build()).thenReturn(bucketInfo);

    Throwable exception = new StorageException(403, null);
    Mockito.when(storage.create(Mockito.any(BucketInfo.class))).thenThrow(exception);

    BigQueryTarget bqTarget = new BigQueryTarget(conf);
    try {
      //IO Exception thrown is wrapped by Failsafe.Failsafe wraps checked exceptions.
      exceptionRule.expect(FailsafeException.class);
      bqTarget.createConsumer(deltaTargetContext);
    } finally {
      //Verify at least 1 retry
      Mockito.verify(storage, Mockito.atLeast(2)).create(Mockito.any(BucketInfo.class));
    }
  }

  @Test
  public void testCreateConsumerRetryableFailureForGcsGet() throws Exception {
    Mockito.when(options.getService()).thenReturn(storage);
    Mockito.when(builder.build()).thenReturn(options);

    Throwable exception = new StorageException(500, null);
    Mockito.when(storage.get(Mockito.anyString())).thenThrow(exception);

    BigQueryTarget bqTarget = new BigQueryTarget(conf);
    try {
      exceptionRule.expect(exception.getClass());
      bqTarget.createConsumer(deltaTargetContext);
    } finally {
      //Verify at least 1 retry
      Mockito.verify(storage, Mockito.atLeast(2)).get(Mockito.nullable(String.class));
    }
  }

  @Test
  public void testCreateConsumerNonRetryableFailure() throws Exception {
    Mockito.when(options.getService()).thenReturn(storage);
    Mockito.when(builder.build()).thenReturn(options);

    Throwable exception = new StorageException(501, null);
    Mockito.when(storage.get(Mockito.anyString())).thenThrow(exception);

    BigQueryTarget bqTarget = new BigQueryTarget(conf);
    try {
      exceptionRule.expect(exception.getClass());
      bqTarget.createConsumer(deltaTargetContext);
    } finally {
      //Verify no retry
      Mockito.verify(storage, Mockito.times(1)).get(Mockito.nullable(String.class));
    }
  }

}
