/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.indexes;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

final class ImmutableEmptyIndexImpl<V> implements StoreIndex<V> {

  private final ElementSerializer<V> serializer;

  ImmutableEmptyIndexImpl(ElementSerializer<V> serializer) {
    this.serializer = serializer;
  }

  @Override
  public boolean isModified() {
    return false;
  }

  @Override
  public StoreIndex<V> loadIfNecessary(Set<StoreKey> keys) {
    return this;
  }

  @Override
  public boolean isLoaded() {
    return true;
  }

  @Override
  public StoreIndex<V> asMutableIndex() {
    return newStoreIndex(serializer);
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public List<StoreIndex<V>> divide(int parts) {
    throw new UnsupportedOperationException("Operation not supported for non-mutable indexes");
  }

  @Override
  public List<StoreIndex<V>> stripes() {
    return emptyList();
  }

  @Override
  public int elementCount() {
    return 0;
  }

  @Override
  public int estimatedSerializedSize() {
    return 1;
  }

  @Override
  public boolean add(@Nonnull StoreIndexElement<V> element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateAll(Function<StoreIndexElement<V>, V> updater) {}

  @Override
  public boolean remove(@Nonnull StoreKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(@Nonnull StoreKey key) {
    return false;
  }

  @Nullable
  @Override
  public StoreIndexElement<V> get(@Nonnull StoreKey key) {
    return null;
  }

  @Nullable
  @Override
  public StoreKey first() {
    return null;
  }

  @Nullable
  @Override
  public StoreKey last() {
    return null;
  }

  @Override
  public List<StoreKey> asKeyList() {
    return emptyList();
  }

  @Nonnull
  @Override
  public Iterator<StoreIndexElement<V>> iterator(
      @Nullable StoreKey begin, @Nullable StoreKey end, boolean prefetch) {
    return emptyIterator();
  }

  @Nonnull
  @Override
  public ByteString serialize() {
    ByteBuffer target = ByteBuffer.allocate(estimatedSerializedSize());

    // Serialized segment index version
    target.put((byte) 1);

    target.flip();
    return unsafeWrap(target);
  }
}
