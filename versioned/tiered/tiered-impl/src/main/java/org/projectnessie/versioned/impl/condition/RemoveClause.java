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
package org.projectnessie.versioned.impl.condition;

import org.immutables.value.Value;

@Value.Immutable
public abstract class RemoveClause implements UpdateClause {
  public abstract ExpressionPath getPath();

  @Override
  public RemoveClause alias(AliasCollector c) {
    return ImmutableRemoveClause.builder().path(getPath().alias(c)).build();
  }

  public static RemoveClause of(ExpressionPath path) {
    return ImmutableRemoveClause.builder().path(path).build();
  }

  @Override
  public Type getType() {
    return Type.REMOVE;
  }

  @Override
  public String toClauseString() {
    return getPath().asString();
  }


}
