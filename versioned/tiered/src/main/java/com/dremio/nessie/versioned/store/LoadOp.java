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
package com.dremio.nessie.versioned.store;

import com.dremio.nessie.tiered.builder.BaseConsumer;

public abstract class LoadOp<C extends BaseConsumer<C>> {
  private final ValueType type;
  private final Id id;

  /**
   * Create a load op.
   * @param type The value type that will be loaded.
   * @param id The id of the value.
   */
  public LoadOp(ValueType type, Id id) {
    this.type = type;
    this.id = id;
  }

  /**
   * Users of a {@link LoadOp} (the store implementations) can deserialize their representation
   * into the {@link BaseConsumer} returned by this method and call {@link #done()} afterwards.
   *
   * @return receiver of the "properties"
   */
  public abstract C getReceiver();

  /**
   * Users of a {@link LoadOp} (the store implementations) can deserialize their representation
   * into the {@link BaseConsumer} returned by {@link #getReceiver()} and call this method afterwards.
   */
  public abstract void done();

  public Id getId() {
    return id;
  }

  public ValueType getValueType() {
    return type;
  }

  @Override
  public String toString() {
    return "LoadOp [type=" + type + ", id=" + id + "]";
  }
}
