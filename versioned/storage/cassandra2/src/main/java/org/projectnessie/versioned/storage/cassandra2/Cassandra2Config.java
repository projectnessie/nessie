/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cassandra2;

import java.time.Duration;

public interface Cassandra2Config {

  /** Timeout used when creating tables. */
  Duration ddlTimeout();

  /** Timeout used for queries and updates. */
  Duration dmlTimeout();

  String DEFAULT_DDL_TIMEOUT = "PT5S";

  String DEFAULT_DML_TIMEOUT = "PT3S";
}
