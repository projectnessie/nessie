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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.Fragment;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

class InternalFragment extends PersistentBase<Fragment> {

  private final List<InternalKey> keys;

  InternalFragment(List<InternalKey> keys) {
    super();
    this.keys = ImmutableList.copyOf(keys);
  }

  InternalFragment(Id id, List<InternalKey> keys, Long dt) {
    super(id, dt);
    this.keys = ImmutableList.copyOf(keys);
  }

  @Override
  Id generateId() {
    return Id.build(h -> keys.forEach(k -> InternalKey.addToHasher(k, h)));
  }

  List<InternalKey> getKeys() {
    return keys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InternalFragment fragment = (InternalFragment) o;
    return Objects.equal(keys, fragment.keys);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(keys);
  }

  @Override
  Fragment applyToConsumer(Fragment consumer) {
    return super.applyToConsumer(consumer)
        .keys(keys.stream().map(InternalKey::toKey));
  }

  /**
   * Implements {@link Fragment} to build a {@link InternalFragment} object.
   */
  // Needs to be a package private class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  static class Builder extends EntityBuilder<InternalFragment, Fragment> implements Fragment {

    private List<InternalKey> keys;

    @Override
    public Builder keys(Stream<Key> keys) {
      checkCalled(this.keys, "keys");
      this.keys = keys.map(InternalKey::new).collect(Collectors.toList());
      return this;
    }

    @Override
    InternalFragment build() {
      // null-id is allowed (will be generated)
      checkSet(keys, "keys");

      return new InternalFragment(id, keys, dt);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public EntityType<Fragment, InternalFragment, InternalFragment.Builder> getEntityType() {
    return EntityType.KEY_FRAGMENT;
  }
}
