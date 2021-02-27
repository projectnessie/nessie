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
package org.projectnessie.versioned.tiered.gc;

import org.apache.spark.sql.api.java.UDF2;

/**
 * Spark function to determine whether a particular L1 is still considered "referenced".
 */
class GcPolicy implements UDF2<String, Long, Boolean> {
  private static final long serialVersionUID = 3412048190607652836L;

  private final long youngestAllowedMicros;

  public GcPolicy(long youngestAllowedMicros) {
    this.youngestAllowedMicros = youngestAllowedMicros;
  }

  @Override
  public Boolean call(String name, Long dt) throws Exception {
    if (dt == null || dt == 0L) {
      return true;
    }
    return dt >= youngestAllowedMicros;
  }

}
