/*
 *
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.common.base.Strings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BigQueryUtilsTest {

  @Test
  public void testNormalizeDataSetOrTableName() {
    // only contains number and letter
    assertEquals("a2fs", BigQueryUtils.normalizeDatasetOrTableName("a2fs"));
    // only contains number and letter start with number
    assertEquals("2fas", BigQueryUtils.normalizeDatasetOrTableName("2fas"));
    // only contains number and letter and length is 1024
    String name = Strings.repeat("a1", 512);
    assertEquals(name, BigQueryUtils.normalizeDatasetOrTableName(name));
    // only contains number and letter and length is 1026
    name = Strings.repeat("a1", 513);
    assertEquals(name.substring(0, 1024), BigQueryUtils.normalizeDatasetOrTableName(name));
    // contains invalid character
    assertEquals("ab_c", BigQueryUtils.normalizeDatasetOrTableName("ab?/c"));
    // contains space
    assertEquals("a2_fs", BigQueryUtils.normalizeFieldName("a2 fs"));

  }

  @Test
  public void testNormalizeFieldName() {
    // only contains number and letter
    assertEquals("a2fs", BigQueryUtils.normalizeFieldName("a2fs"));
    // only contains number and letter start with number
    assertEquals("_2fas", BigQueryUtils.normalizeFieldName("2fas"));
    // only contains number and letter and length is 128
    String name = Strings.repeat("a1", 64);
    assertEquals(name, BigQueryUtils.normalizeFieldName(name));
    // only contains number and letter, starts with number and length is 128
    name = Strings.repeat("1a", 64);
    assertEquals("_" + name.substring(0, 127), BigQueryUtils.normalizeFieldName(name));
    // only contains number and letter and length is 130
    name = Strings.repeat("a1", 65);
    assertEquals(name.substring(0, 128), BigQueryUtils.normalizeFieldName(name));
    // contains invalid character
    assertEquals("ab_c", BigQueryUtils.normalizeFieldName("ab?/c"));
    // contains space
    assertEquals("a2_fs", BigQueryUtils.normalizeFieldName("a2 fs"));
  }
}
