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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.ImmutableKey;
import com.dremio.nessie.versioned.Key;
import com.google.common.base.Suppliers;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A version of key that memoizes the id of the key according to sha256 hashing.
 */
class InternalKey implements Comparable<InternalKey>, HasId {

  private final Key delegate;
  private final Supplier<Id> idMemo = Suppliers.memoize(() -> Id.build(h -> addToHasher(this, h)));

  public InternalKey(Key delegate) {
    super();
    this.delegate = delegate;
  }

  public InternalKey(List<String> elements) {
    this.delegate = ImmutableKey.builder().addAllElements(elements).build();
  }

  @Override
  public int compareTo(InternalKey o) {
    return delegate.compareTo(o.delegate);
  }

  @Override
  public Id getId() {
    return idMemo.get();
  }

  public int getL1Position() {
    return Math.abs(Ints.fromByteArray(idMemo.get().getValue().substring(0, 4).toByteArray()) % L1.SIZE);
  }

  public int getL2Position() {
    return Math.abs(Ints.fromByteArray(idMemo.get().getValue().substring(4, 8).toByteArray()) % L2.SIZE);
  }

  @Override
  public int hashCode() {
    return addToHasher(this, Hashing.murmur3_32().newHasher()).hash().asInt();
  }

  public AttributeValue toAttributeValue() {
    return AttributeValue.builder().l(getElements().stream().map(s -> AttributeValue.builder().s(s).build()).collect(Collectors.toList())).build();
  }

  public static InternalKey fromAttributeValue(AttributeValue value) {
    return new InternalKey(ImmutableKey.builder().addAllElements(value.l().stream().map(AttributeValue::s).collect(Collectors.toList())).build());
  }

  public List<String> getElements() {
    return delegate.getElements();
  }

  @Override
  public boolean equals(Object obj) {
    if(obj == null) {
      return false;
    }
    if( !(obj instanceof InternalKey) ) {
      return false;
    }
    Key other = (Key) obj;
    List<String> thisLower = delegate.getElements().stream().map(String::toLowerCase).collect(Collectors.toList());
    List<String> otherLower = other.getElements().stream().map(String::toLowerCase).collect(Collectors.toList());
    return thisLower.equals(otherLower);
  }

  public static Hasher addToHasher(InternalKey key, Hasher hasher) {
    key.delegate.getElements().forEach(s -> hasher.putString(s.toLowerCase(), StandardCharsets.UTF_8));
    return hasher;
  }

}
