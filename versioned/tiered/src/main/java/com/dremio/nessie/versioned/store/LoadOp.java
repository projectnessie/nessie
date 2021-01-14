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

import java.util.function.Consumer;

import com.dremio.nessie.tiered.builder.BaseConsumer;

public class LoadOp<C extends BaseConsumer<C>> {
  private final ValueType type;
  private final Id id;
  private final C receiver;
  private final Consumer<C> doneCallback;

  /**
   * Create a load op.
   * @param type The value type that will be loaded.
   * @param id The id of the value.
   * @param receiver The {@link BaseConsumer instance} that will receive the entity load events.
   * @param doneCallback Gets called with the value passed in as {@code producer}, when deserialization has finished.
   */
  public LoadOp(ValueType type, Id id, C receiver, Consumer<C> doneCallback) {
    this.type = type;
    this.id = id;
    this.receiver = receiver;
    this.doneCallback = doneCallback;
  }

  /**
   * Store implementations call this method with a callback that must be called to
   * perform the actual deserialization.
   *
   * @param deserializer callback that accepts a {@link BaseConsumer}
   */
  public void deserialize(Consumer<C> deserializer) {
    deserializer.accept(receiver);
    doneCallback.accept(receiver);
  }

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
