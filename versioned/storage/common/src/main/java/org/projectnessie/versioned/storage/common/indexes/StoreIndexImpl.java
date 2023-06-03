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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.binarySearch;
import static java.util.Collections.singletonList;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.findPositionAfterKey;
import static org.projectnessie.versioned.storage.common.util.Ser.putVarInt;
import static org.projectnessie.versioned.storage.common.util.Ser.readVarInt;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * Implementation of {@link StoreIndex} that implements "version 1 serialization" of
 * key-index-segments.
 *
 * <p>"Version 1" uses a diff-like encoding to compress keys and a custom var-int encoding. {@link
 * StoreIndexElement}s are serialized in their natural order.
 *
 * <p>{@link StoreKey}s are serialized by serializing each element's UTF-8 representation with a
 * terminating {@code 0} byte, and the whole key terminated by a trailing {@code 0} byte. Empty key
 * elements are not allowed. The total serialized size of a key must not exceed {@value
 * #MAX_KEY_BYTES}.
 *
 * <p>Key serialization considers the previously serialized key - common prefixes are not
 * serialized. One var-ints is used to implement a diff-ish encoding: The var-int represents the
 * number of trailing bytes to strip from the previous key, then all bytes until the
 * double-zero-bytes end-marker is appended. For example, if the previous key was {@code
 * aaa.bbb.TableFoo} and the "current" key is {@code aaa.bbb.TableBarBaz}, the last three bytes of
 * {@code aaa.bbb.TableFoo} need to be removed, resulting in {@code aaa.bbb.Table} and 6 more bytes
 * ({@code BarBaz}) need to be appended to form the next key {@code aaa.bbb.TableBarBaz}. In this
 * case, the var-int {@code 3} is written plus the serialized representation of the 6 bytes to
 * represent {@code BarBaz} are serialized. The first serialized key is written in its entirety,
 * omitting the var-int that represents the number of bytes to "strip" from the previous key.
 *
 * <p>Using var-ints to represent the number of bytes to "strip" from the previous key is more space
 * efficient, because it is very likely that two keys serialized after each other share a long
 * common prefix, especially since keys are serialized in their natural order.
 *
 * <p>The serialized key-index does not write any length information of the individual elements or
 * parts (like the {@link StoreKey} or value) to reduce the space required for serialization.
 *
 * <h2>This implementation</h2>
 *
 * <p>This implementation is just an idea, not an actual proposal.
 *
 * <h2>Other ideas</h2>
 *
 * <p>There are other possible ideas and approaches to implement a serializable index of {@link
 * StoreKey} to something else:
 *
 * <ul>
 *   <li>Assumption (not true): Store serialized keys <em>separate</em> from other binary content,
 *       assuming that {@link StoreKey}s are compressible and the compression ratio of a set of keys
 *       is pretty good, unlike for example hash values, which are rather random and serialization
 *       likely does not benefit from compression.
 *       <p><em>RESULT</em> Experiment with >80000 words (each at least 10 chars long) for key
 *       elements: compression (gzip) of a key-to-commit-entry index (32 byte hashes) with
 *       interleaved key and value saves about 15% - the compressed ratio with keys first is only
 *       marginally better (approx 20%), so it is not worth the extra complexity.
 *   <li>Assumption (not true): Compressing the key-indexes helps reducing database round trips a
 *       lot. As mentioned above, the savings of compression are around 15-22%. We can assume that
 *       the network traffic to the database is already compressed, so we do not save bandwidth - it
 *       might save one (or two) row reads of a bulk read. The savings do not feel worth the extra
 *       complexity.
 *   <li>Have another implementation that is similar to this one, but uses a {@link
 *       java.util.TreeMap} to build indexes, when there are many elements to add to the index
 *   <li>Cross check whether the left-truncation used in the serialized representation of this
 *       implementation is really legit in real life. <em>It still feels valid and legit and
 *       efficient.</em>
 *   <li>Add some checksum (e.g. {@link java.util.zip.CRC32C}, preferred, or {@link
 *       java.util.zip.CRC32}) to the serialized representation?
 * </ul>
 */
final class StoreIndexImpl<V> implements StoreIndex<V> {

  static final int MAX_KEY_BYTES = 4096;

  /**
   * Assume 4 additional bytes for each added entry: 2 bytes for the "strip" and 2 bytes for the
   * "add" var-ints.
   */
  private static final int ASSUMED_PER_ENTRY_OVERHEAD = 2 + 2;

  private static final boolean SERIALIZE_V2 =
      !Boolean.getBoolean("nessie.internal.store-index-old-format");

  public static final Comparator<StoreIndexElement<?>> KEY_COMPARATOR =
      Comparator.comparing(StoreIndexElement::key);

  /**
   * Serialized size of the index at the time when the {@link #StoreIndexImpl(List, int,
   * ElementSerializer, boolean)} constructor has been called.
   *
   * <p>This field is used to <em>estimate</em> the serialized size when this object is serialized
   * again, including modifications.
   */
  private final int originalSerializedSize;

  private int estimatedSerializedSizeDiff;
  private final List<StoreIndexElement<V>> elements;
  private final ElementSerializer<V> serializer;

  private boolean modified;
  private ObjId objId;

  // NOTE: The implementation uses j.u.ArrayList to optimize for reads. Additions to this data
  // structure are rather inefficient, when elements need to be added "in the middle" of the
  // 'elements' j.u.ArrayList.

  StoreIndexImpl(ElementSerializer<V> serializer) {
    this(new ArrayList<>(), 1, serializer, false);
  }

  StoreIndexImpl(
      List<StoreIndexElement<V>> elements,
      int originalSerializedSize,
      ElementSerializer<V> serializer,
      boolean modified) {
    this.elements = elements;
    this.originalSerializedSize = originalSerializedSize;
    this.serializer = serializer;
    this.modified = modified;
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  @Override
  public ObjId getObjId() {
    return objId;
  }

  @Override
  public StoreIndex<V> setObjId(ObjId objId) {
    this.objId = objId;
    return this;
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
    return this;
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public List<StoreIndex<V>> divide(int parts) {
    List<StoreIndexElement<V>> elems = elements;
    int size = elems.size();
    checkArgument(
        parts > 0 && parts <= size,
        "Number of parts %s must be greater than 0 and less or equal to number of elements %s",
        parts,
        size);
    int partSize = size / parts;
    int serializedMax = originalSerializedSize + estimatedSerializedSizeDiff;

    List<StoreIndex<V>> result = new ArrayList<>(parts);
    int index = 0;
    for (int i = 0; i < parts; i++) {
      int end = i < parts - 1 ? index + partSize : elems.size();
      List<StoreIndexElement<V>> partElements = new ArrayList<>(elements.subList(index, end));
      StoreIndexImpl<V> part = new StoreIndexImpl<>(partElements, serializedMax, serializer, true);
      result.add(part);
      index = end;
    }
    return result;
  }

  @Override
  public List<StoreIndex<V>> stripes() {
    return singletonList(this);
  }

  @Override
  public int elementCount() {
    return elements.size();
  }

  @Override
  public void updateAll(Function<StoreIndexElement<V>, V> updater) {
    List<StoreIndexElement<V>> e = elements;
    int size = e.size();
    for (int i = 0; i < size; i++) {
      StoreIndexElement<V> el = e.get(i);
      V updated = updater.apply(el);
      if (updated != el) {
        modified = true;
        int oldSerializedSize = serializer.serializedSize(el.content());
        if (updated == null) {
          e.remove(i);
          i--;
          size--;
        } else {
          int newSerializedSize = serializer.serializedSize(updated);
          estimatedSerializedSizeDiff += newSerializedSize - oldSerializedSize;
          e.set(i, indexElement(el.key(), updated));
        }
      }
    }
  }

  @Override
  public boolean add(@Nonnull @jakarta.annotation.Nonnull StoreIndexElement<V> element) {
    modified = true;
    List<StoreIndexElement<V>> e = elements;
    int idx = search(e, element);
    int elementSerializedSize = serializer.serializedSize(element.content());
    if (idx >= 0) {
      // exact match, key already in segment
      StoreIndexElement<V> prev = e.get(idx);

      int prevSerializedSize = serializer.serializedSize(prev.content());
      estimatedSerializedSizeDiff += elementSerializedSize - prevSerializedSize;

      e.set(idx, element);
      return false;
    }

    estimatedSerializedSizeDiff += addElementDiff(element, elementSerializedSize);

    int insertionPoint = -idx - 1;
    if (insertionPoint == e.size()) {
      e.add(element);
    } else {
      e.add(insertionPoint, element);
    }
    return true;
  }

  private static <V> int addElementDiff(StoreIndexElement<V> element, int elementSerializedSize) {
    return serializedSize(element.key()) + ASSUMED_PER_ENTRY_OVERHEAD + elementSerializedSize;
  }

  @Override
  public boolean remove(@Nonnull @jakarta.annotation.Nonnull StoreKey key) {
    List<StoreIndexElement<V>> e = elements;
    int idx = search(e, key);
    if (idx < 0) {
      return false;
    }

    modified = true;

    StoreIndexElement<V> element = e.remove(idx);

    estimatedSerializedSizeDiff -= removeSizeDiff(element);

    return true;
  }

  private int removeSizeDiff(StoreIndexElement<V> element) {
    return 2 + serializer.serializedSize(element.content());
  }

  @Override
  public boolean contains(@Nonnull @jakarta.annotation.Nonnull StoreKey key) {
    int idx = search(elements, key);
    return idx >= 0;
  }

  @Override
  public @Nullable @jakarta.annotation.Nullable StoreIndexElement<V> get(
      @Nonnull @jakarta.annotation.Nonnull StoreKey key) {
    List<StoreIndexElement<V>> e = elements;
    int idx = search(e, key);
    if (idx < 0) {
      return null;
    }
    return e.get(idx);
  }

  @Nullable
  @jakarta.annotation.Nullable
  @Override
  public StoreKey first() {
    List<StoreIndexElement<V>> e = elements;
    return e.isEmpty() ? null : e.get(0).key();
  }

  @Nullable
  @jakarta.annotation.Nullable
  @Override
  public StoreKey last() {
    List<StoreIndexElement<V>> e = elements;
    return e.isEmpty() ? null : e.get(e.size() - 1).key();
  }

  @Override
  public @Nonnull @jakarta.annotation.Nonnull Iterator<StoreIndexElement<V>> iterator(
      @Nullable @jakarta.annotation.Nullable StoreKey begin,
      @Nullable @jakarta.annotation.Nullable StoreKey end,
      boolean prefetch) {
    List<StoreIndexElement<V>> e = elements;

    if (begin == null && end == null) {
      return e.iterator();
    }

    boolean prefix = begin != null && begin.equals(end);
    int fromIdx = begin != null ? iteratorIndex(begin, 0) : 0;
    int toIdx = !prefix && end != null ? iteratorIndex(end, 1) : e.size();

    checkArgument(toIdx >= fromIdx, "'to' must be greater than 'from'");

    e = e.subList(fromIdx, toIdx);
    Iterator<StoreIndexElement<V>> base = e.iterator();
    return prefix
        ? new AbstractIterator<StoreIndexElement<V>>() {

          @Override
          protected StoreIndexElement<V> computeNext() {
            if (!base.hasNext()) {
              return endOfData();
            }
            StoreIndexElement<V> v = base.next();
            if (!v.key().startsWith(begin)) {
              return endOfData();
            }
            return v;
          }
        }
        : base;
  }

  private int iteratorIndex(StoreKey from, int exactAdd) {
    int fromIdx = search(elements, from);
    if (fromIdx < 0) {
      fromIdx = -fromIdx - 1;
    } else {
      fromIdx += exactAdd;
    }
    return fromIdx;
  }

  @Override
  @VisibleForTesting
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StoreIndexImpl)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    StoreIndexImpl<V> that = (StoreIndexImpl<V>) o;
    return elements.equals(that.elements);
  }

  @Override
  @VisibleForTesting
  public int hashCode() {
    return elements.hashCode();
  }

  @Override
  public String toString() {
    StoreKey f = first();
    StoreKey l = last();
    String fk = f != null ? f.toString() : "";
    String lk = l != null ? l.toString() : "";
    return "StoreIndexImpl{size=" + elementCount() + ", first=" + fk + ", last=" + lk + "}";
  }

  @Override
  public List<StoreKey> asKeyList() {
    return new AbstractList<StoreKey>() {
      @Override
      public StoreKey get(int index) {
        return elements.get(index).key();
      }

      @Override
      public int size() {
        return elements.size();
      }
    };
  }

  @Override
  public int estimatedSerializedSize() {
    return originalSerializedSize + estimatedSerializedSizeDiff + 1;
  }

  @Override
  public @Nonnull @jakarta.annotation.Nonnull ByteString serialize() {
    ByteBuffer target = ByteBuffer.allocate(estimatedSerializedSize());

    // Serialized segment index version
    if (SERIALIZE_V2) {
      target.put((byte) 2);
      putVarInt(target, elementCount());
    } else {
      target.put((byte) 1);
    }

    ByteBuffer previousKey = null;

    // This buffer's backs the currently serialized key - use with care!
    // Having this "singleton" instance massively reduces GC alloc/churn rate.
    ByteBuffer serializationBuffer = newKeyBuffer();

    @SuppressWarnings("UnnecessaryLocalVariable")
    ElementSerializer<V> ser = serializer;

    for (StoreIndexElement<V> el : this) {
      StoreKey key = el.key();

      previousKey = serializeKey(previousKey, key, target, serializationBuffer);

      ser.serialize(el.content(), target);
    }

    target.flip();

    return unsafeWrap(target);
  }

  private ByteBuffer serializeKey(
      ByteBuffer previousKey, StoreKey key, ByteBuffer target, ByteBuffer serializationBuffer) {
    ByteBuffer keyBuf = key.serialize(serializationBuffer);
    int keyPos = keyBuf.position();
    if (previousKey != null) {
      int mismatch = previousKey.mismatch(keyBuf);
      checkState(mismatch != -1, "Previous and current keys must not be equal");
      int strip = previousKey.remaining() - mismatch;
      putVarInt(target, strip);
      keyBuf.position(keyPos + mismatch);
    } else {
      previousKey = newKeyBuffer();
    }
    target.put(keyBuf);

    previousKey.clear();
    keyBuf.position(keyPos);
    previousKey.put(keyBuf);
    previousKey.flip();

    return previousKey;
  }

  static <V> StoreIndex<V> deserializeStoreIndex(ByteBuffer serialized, ElementSerializer<V> ser) {
    byte version = serialized.get();
    checkArgument(
        version == 1 || version == 2, "Unsupported serialized representation of KeyIndexSegment");

    int posPre = serialized.position();

    List<StoreIndexElement<V>> elements =
        version >= 2 ? new ArrayList<>(readVarInt(serialized)) : new ArrayList<>();

    // This buffer holds the previous key, reused.
    ByteBuffer previousKey = newKeyBuffer();

    boolean first = true;
    while (serialized.remaining() > 0) {
      int strip = first ? 0 : readVarInt(serialized);
      first = false;

      // strip
      previousKey.position(previousKey.position() - strip);
      previousKey.limit(MAX_KEY_BYTES);
      // add
      int limitSave = serialized.limit();
      previousKey.put(serialized.limit(findPositionAfterKey(serialized)));
      serialized.limit(limitSave);
      // read key
      previousKey.flip();
      StoreKey key = StoreKey.deserializeKey(previousKey);

      // read value
      V value = ser.deserialize(serialized);
      elements.add(indexElement(key, value));
    }

    int len = serialized.position() - posPre;
    return new StoreIndexImpl<>(elements, len, ser, false);
  }

  @VisibleForTesting
  static int serializedSize(StoreKey key) {
    // 1st byte: number of elements
    int size = 2;
    size += key.rawString().getBytes(StandardCharsets.UTF_8).length;
    return size;
  }

  @VisibleForTesting
  static ByteBuffer newKeyBuffer() {
    return ByteBuffer.allocate(MAX_KEY_BYTES);
  }

  private static <V> int search(
      List<StoreIndexElement<V>> e, @Nonnull @jakarta.annotation.Nonnull StoreKey key) {
    // Need a StoreIndexElement for the sake of 'binarySearch()' (the content value isn't used)
    return search(e, indexElement(key, ""));
  }

  private static <V> int search(List<StoreIndexElement<V>> e, StoreIndexElement<?> element) {
    return binarySearch(e, element, KEY_COMPARATOR);
  }
}
