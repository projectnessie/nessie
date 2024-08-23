/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.bigtable;

import com.google.protobuf.ByteString;
import java.time.Duration;

final class BigTableConstants {

  static final String TABLE_REFS = "refs";
  static final String TABLE_OBJS = "objs";

  static final String FAMILY_REFS = "r";
  static final String FAMILY_OBJS = "o";

  static final ByteString QUALIFIER_OBJ_TYPE = ByteString.copyFromUtf8("t");
  static final ByteString QUALIFIER_OBJ_VERS = ByteString.copyFromUtf8("V");
  static final ByteString QUALIFIER_OBJ_REFERENCED = ByteString.copyFromUtf8("z");
  static final ByteString QUALIFIER_OBJS = ByteString.copyFromUtf8("o");
  static final ByteString QUALIFIER_REFS = ByteString.copyFromUtf8("r");
  // regex for scan
  static final ByteString QUALIFIER_OBJS_OR_VERS = ByteString.copyFromUtf8("o|V");

  // Tue Apr 7 08:14:21 2020 +0200
  static final long CELL_TIMESTAMP = 1586232861000L;

  static final int MAX_PARALLEL_READS = 5;
  static final int MAX_BULK_READS = 100;
  static final int MAX_BULK_MUTATIONS = 1000;

  static final Duration DEFAULT_BULK_READ_TIMEOUT = Duration.ofSeconds(5);

  private BigTableConstants() {}
}
