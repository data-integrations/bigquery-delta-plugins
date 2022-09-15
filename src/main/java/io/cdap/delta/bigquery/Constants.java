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

/**
 * Class defining constants.
 */
public final class Constants {
  public static final String SEQUENCE_NUM = "_sequence_num";
  public static final String SOURCE_TIMESTAMP = "_source_timestamp";
  public static final String IS_DELETED = "_is_deleted";
  public static final String ROW_ID = "_row_id";
  public static final String OPERATION = "_op";
  public static final String BATCH_ID = "_batch_id";
  public static final String SORT_KEYS = "_sort";
  public static final String SORT_KEY_FIELD = "_key";

  private Constants() {}
}
