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

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.SourceProperties;
import io.cdap.delta.api.SourceTable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Mock version of the context.
 */
public class MockContext implements DeltaTargetContext {
  public static final DeltaTargetContext INSTANCE = new MockContext(0, new HashMap());
  private final int maxRetrySeconds;
  private final Map runtimeArguments;

  public MockContext(int maxRetrySeconds, Map runtimeArguments) {
    this.maxRetrySeconds = maxRetrySeconds;
    this.runtimeArguments = runtimeArguments;
  }

  @Override
  public void incrementCount(DMLOperation operation) {
    // no-op
  }

  @Override
  public void incrementCount(DDLOperation operation) {
    // no-op
  }

  @Override
  public void commitOffset(Offset offset, long l) {
    // no-op
  }

  @Override
  public void setTableError(String s, String s1, ReplicationError replicationError) {
    // no-op
  }

  @Override
  public void setTableError(String s, @Nullable String s1, String s2,
                            ReplicationError replicationError) throws IOException {

  }

  @Override
  public void setTableReplicating(String s, String s1) {
    // no-op
  }

  @Override
  public void setTableSnapshotting(String s, String s1) {
    // no-op
  }

  @Override
  public void dropTableState(String s, String s1) {
    // no-op
  }

  @Override
  public SourceProperties getSourceProperties() {
    return null;
  }

  @Override
  public String getApplicationName() {
    return null;
  }

  @Override
  public String getRunId() {
    return null;
  }

  @Override
  public Metrics getMetrics() {
    return null;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  @Override
  public int getInstanceId() {
    return 1;
  }

  @Override
  public int getMaxRetrySeconds() {
    return maxRetrySeconds;
  }

  @Nullable
  @Override
  public byte[] getState(String s) {
    return new byte[0];
  }

  @Override
  public void putState(String s, byte[] bytes) {
    // no-op
  }

  @Override
  public DeltaPipelineId getPipelineId() {
    return new DeltaPipelineId("default", "app", 0L);
  }

  @Override
  public Set<SourceTable> getAllTables() {
    return Collections.emptySet();
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return null;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException {
    return null;
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return null;
  }

  @Override
  public <T> T newPluginInstance(String pluginId) {
    return null;
  }

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException {
    return null;
  }
}
