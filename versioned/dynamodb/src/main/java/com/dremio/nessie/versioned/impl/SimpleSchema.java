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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Abstract class for converting to/from an object to a Map&gt;String, AttributeValue&lt;.
 *
 * <p>Inspired by Dynamo's extended library and originally extended from it.
 * @param <T> The value type to be serialized/deserialized.
 */
abstract class SimpleSchema<T> {

  public SimpleSchema(Class<T> clazz) {
  }

  public Map<String, AttributeValue> itemToMap(T item, Collection<String> attributes) {
    Set<String> include = ImmutableSet.copyOf(attributes);
    return Maps.filterKeys(itemToMap(item, true), k -> include.contains(k));
  }

  public abstract Map<String, AttributeValue> itemToMap(T item, boolean ignoreNulls);

  public final T mapToItem(Map<String, AttributeValue> attributeMap) {
    return deserialize(new NullAlertingMap(attributeMap));
  }

  protected abstract T deserialize(Map<String, AttributeValue> attributeMap);


  public AttributeValue attributeValue(T item, String key) {
    return itemToMap(item, true).get(key);
  }

  /**
   * A map which throws if a requested value is missing rather than returning null.
   */
  private static class NullAlertingMap implements Map<String, AttributeValue> {

    private final Map<String, AttributeValue> delegate;

    public NullAlertingMap(Map<String, AttributeValue> delegate) {
      this.delegate = delegate;
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return delegate.containsValue(value);
    }

    @Override
    public AttributeValue get(Object key) {
      AttributeValue value = delegate.get(key);
      if (value == null) {
        throw new NullPointerException(String.format("Unable to find '%s' in: %s.", key, this));
      }
      return value;
    }

    @Override
    public AttributeValue put(String key, AttributeValue value) {
      return delegate.put(key, value);
    }

    @Override
    public AttributeValue remove(Object key) {
      return delegate.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends AttributeValue> m) {
      delegate.putAll(m);
    }

    @Override
    public void clear() {
      delegate.clear();
    }

    @Override
    public Set<String> keySet() {
      return delegate.keySet();
    }

    @Override
    public Collection<AttributeValue> values() {
      return delegate.values();
    }

    @Override
    public Set<Entry<String, AttributeValue>> entrySet() {
      return delegate.entrySet();
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }
}
