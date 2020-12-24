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
package com.dremio.nessie.versioned.impl.condition;

import com.dremio.nessie.versioned.impl.condition.AliasCollector.Aliasable;

public interface UpdateClause extends Aliasable<UpdateClause> {

  enum Type {
    SET, REMOVE
  }

  Type getType();

  String toClauseString();

  /**
   * Default implementation for visitation.
   * @param visitor the visitor that will be invoked.
   * @param <T> the type of the returned value.
   * @return the possibly transformed value resulting from the visitation.
   */
  default <T> T accept(UpdateClauseVisitor<T> visitor) {
    throw new IllegalArgumentException();
  }
}
