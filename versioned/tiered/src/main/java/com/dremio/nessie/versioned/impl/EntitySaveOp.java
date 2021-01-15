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
package com.dremio.nessie.versioned.impl;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.ValueType;

final class EntitySaveOp<C extends BaseConsumer<C>, E extends PersistentBase<C>> extends SaveOp<C> {
  private final E value;

  EntitySaveOp(ValueType type, E value) {
    super(type, value.getId());
    this.value = value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void serialize(BaseConsumer<C> consumer) {
    value.applyToConsumer((C) consumer);
  }
}
