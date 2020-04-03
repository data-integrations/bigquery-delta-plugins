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
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.ReplicationError;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * No-op version of the context.
 */
public class NoOpContext implements DeltaTargetContext {
  public static final DeltaTargetContext INSTANCE = new NoOpContext();

  @Override
  public void incrementCount(DMLOperation dmlOperation) {
    // no-op
  }

  @Override
  public void incrementCount(DDLOperation ddlOperation) {
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
    return new HashMap<>();
  }

  @Override
  public int getInstanceId() {
    return 1;
  }

  @Override
  public int getMaxRetrySeconds() {
    return 60;
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
