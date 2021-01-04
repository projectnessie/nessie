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
package com.dremio.nessie.tiered.builder;

import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;

/**
 * TODO javadoc.
 * <p>
 * Do not implement this interface in non-abstract implementation classes!
 * </p>
 */
public interface HasIdConsumer<T> {
  /**
   * The id for this consumer.
   *
   * <p>Can be called once.
   * @param id The id.
   * @return This consumer.
   */
  T id(Id id);

  /**
   * Validation helper that checks whether a consumer implementation can handle the given
   * {@link ValueType valueType}.
   *
   * @param valueType the type to check
   * @return {@code true}, if the implementation can handle the given {@code valueType}, {@code false otherwise}
   */
  boolean canHandleType(ValueType valueType);
}
