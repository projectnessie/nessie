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

import com.google.protobuf.ByteString;

/**
 * Base consumer implementation for commit-metadata and binary values.
 * <p>
 * Do not implement this interface in non-abstract implementation classes!
 * </p>
 */
public interface WrappedValueConsumer<C extends WrappedValueConsumer<C>> extends BaseConsumer<C> {
  /**
   * The value for this bytes-value.
   * <p>Must be called exactly once.</p>
   *
   * @param value The value to set.
   * @return This consumer.
   */
  C value(ByteString value);
}
