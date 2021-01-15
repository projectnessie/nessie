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
package com.dremio.nessie.versioned.store.jdbc;

import java.util.HashMap;
import java.util.Map;

import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;

/**
 * Tracks the context/current-state of a change (save/put/update/delete).
 * <p>For "simple" operations (save/put/delete), it's rather a simple holder for {@link SQLChange} and {@link Id}.</p>
 * <p>For "complex" operations (update), this class also tracks the "adjusted index" for array-elemnent-removals.
 * For example, an {@link UpdateExpression} may specify something like "delete branch-commit#0, delete branch-commit#1,
 * set branch-commit#2=blah". The SQL implementation moves the values in the branch-commit-columns from "right to left"
 * for each "delete branch-commit#N" operation, so following updates need to adjust the array-index.</p>
 */
final class UpdateContext {
  final SQLChange change;
  final Id id;
  final Map<String, Integer> indexAdjustments = new HashMap<>();

  UpdateContext(SQLChange change, Id id) {
    this.change = change;
    this.id = id;
  }

  int adjustedIndex(String property) {
    return indexAdjustments.compute(property,
        (k, adj) -> adj != null ? adj : 0);
  }

  void adjustIndex(String property) {
    indexAdjustments.compute(property,
        (k, adj) -> adj != null ? -1 + adj : -1);
  }
}
