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
package org.projectnessie.versioned.impl;

import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.store.Id;


class ValueHolder<V> {

  private final Serializer<V> serializer;
  private V value;
  private WrappedValueBean<?> bean;

  public ValueHolder(Serializer<V> serializer, V value, InternalValue bean) {
    super();
    this.serializer = serializer;
    this.value = value;
    this.bean = bean;
  }

  public V getValue() {
    if (value == null) {
      value = serializer.fromBytes(bean.getBytes());
    }
    return value;
  }

  public WrappedValueBean<?> getPersistentValue() {
    if (bean == null) {
      bean = InternalValue.of(serializer.toBytes(value));
    }
    return bean;
  }

  public Id getId() {
    return getPersistentValue().getId();
  }

  public static <V> ValueHolder<V> of(Serializer<V> serializer, V value) {
    return new ValueHolder<>(serializer, value, null);
  }

  public static <V> ValueHolder<V> of(Serializer<V> serializer, InternalValue bean) {
    return new ValueHolder<>(serializer, null, bean);
  }
}
