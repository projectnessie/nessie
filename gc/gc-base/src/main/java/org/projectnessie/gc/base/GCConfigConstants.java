/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.gc.base;

/** Configuration constants for Nessie GC. */
public final class GCConfigConstants {
  /**
   * Config property name ({@value #CONF_NESSIE_CLIENT_BUILDER_IMPL}) for custom client builder
   * class name.
   */
  public static final String CONF_NESSIE_CLIENT_BUILDER_IMPL = "nessie.client-builder-impl";
  /**
   * Config property name ({@value #CONF_NESSIE_GC_BLOOM_FILTER_SIZE}) for Nessie gc bloom filter
   * expected size per table in a reference.
   */
  public static final String CONF_NESSIE_GC_BLOOM_FILTER_SIZE =
      "nessie.gc.expected.per.table.bloomfilter.size";
  /**
   * Config property name ({@value #CONF_NESSIE_GC_BLOOM_FILTER_FPP}) for Nessie gc bloom filter
   * fpp.
   */
  public static final String CONF_NESSIE_GC_BLOOM_FILTER_FPP =
      "nessie.gc.allowed.fpp.per.table.bloomfilter";
  /**
   * Config property name ({@value #CONF_NESSIE_GC_SPARK_TASK_COUNT}) for Nessie gc spark task
   * parallelism.
   */
  public static final String CONF_NESSIE_GC_SPARK_TASK_COUNT = "nessie.gc.spark.task.count";
  /**
   * Config property name ({@value #CONF_NESSIE_GC_COMMIT_PROTECTION_TIME_IN_HOURS}) for Nessie gc
   * commit protection time in hours, this configuration safeguards fresh contents from getting
   * cleaned up by mistake. Default value is 2 hours if not configured.
   */
  public static final String CONF_NESSIE_GC_COMMIT_PROTECTION_TIME_IN_HOURS =
      "nessie.gc.commit.protection.time.in.hours";

  private GCConfigConstants() {
    // empty
  }
}
