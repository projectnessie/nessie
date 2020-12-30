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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collector;

import com.google.common.base.Preconditions;

public class LoadOp<V extends HasId> {
  private final ValueType type;
  private final Id id;
  private final Consumer<V> consumer;

  /**
   * Create a load op.
   * @param type The value type that will be loaded.
   * @param id The id of the value.
   * @param consumer The consumer who will consume the loaded value.
   */
  public LoadOp(ValueType type, Id id, Consumer<V> consumer) {
    this.type = type;
    this.id = id;
    this.consumer = consumer;
  }

  /** replacement for loaded w/ entity. */
  public void loaded(V load) {
    consumer.accept(load);
  }

  public Id getId() {
    return id;
  }

  public ValueType getValueType() {
    return type;
  }

  public static Collector<LoadOp<?>, ?, LoadOp<?>> toLoadOp() {
    return COLLECTOR;
  }

  /**
   * A collector that combines loadops of the same id and valuetype.
   */
  private static final Collector<LoadOp<?>, OpCollectorState, LoadOp<?>> COLLECTOR = Collector.of(
      OpCollectorState::new,
      (o1, l1) -> o1.plus(l1),
      (o1, o2) -> o1.plus(o2),
      o -> o.build()
      );

  private static class OpCollectorState {

    private ValueType valueType;
    private Id id;
    private List<Consumer<?>> consumers = new ArrayList<>();

    public OpCollectorState plus(OpCollectorState o) {
      if (this.hasValues() && o.hasValues()) {
        Preconditions.checkArgument(this.valueType == o.valueType);
        Preconditions.checkArgument(this.id.equals(o.id));
      }
      OpCollectorState o2 = new OpCollectorState();

      OpCollectorState withV = hasValues() ? this : o;
      o2.valueType = withV.valueType;
      o2.id = withV.id;
      o2.consumers.addAll(this.consumers);
      o2.consumers.addAll(o.consumers);
      return o2;
    }

    public void plus(LoadOp<?> o) {
      if (consumers.isEmpty()) {
        this.valueType = o.getValueType();
        this.id = o.getId();
      } else {
        Preconditions.checkArgument(this.valueType == o.type);
        Preconditions.checkArgument(this.id.equals(o.id));
      }
      consumers.add(o.consumer);
    }

    public boolean hasValues() {
      return !consumers.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public LoadOp<?> build() {
      return new LoadOp<HasId>(valueType, id, v -> {
        for (Consumer<?> c : consumers) {
          ((Consumer<Object>)c).accept(v);
        }
      });
    }
  }


  @Override
  public String toString() {
    return "LoadOp [type=" + type + ", id=" + id + "]";
  }

  public LoadOpKey toKey() {
    return new LoadOpKey(type, id);
  }

  static class LoadOpKey {
    private final ValueType type;
    private final Id id;

    public LoadOpKey(ValueType type, Id id) {
      super();
      this.type = type;
      this.id = id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, type);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      LoadOpKey other = (LoadOpKey) obj;
      return Objects.equals(id, other.id) && type == other.type;
    }

  }

}
