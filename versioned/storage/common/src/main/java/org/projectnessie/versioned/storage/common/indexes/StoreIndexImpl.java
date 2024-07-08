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
import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.deserializeKey;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.skipKey;
import static org.projectnessie.versioned.storage.common.util.Ser.putVarInt;
import static org.projectnessie.versioned.storage.common.util.Ser.readVarInt;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * Implementation of {@link StoreIndex} that implements "version 1 + 2 serialization" of
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

  private static final int CURRENT_STORE_INDEX_VERSION = 2;

  private static final int SERIALIZE_VERSION =
      Integer.getInteger("nessie.internal.store-index-format-version", CURRENT_STORE_INDEX_VERSION);

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

  /**
   * Buffer that holds the raw serialized value of a store index. This buffer's {@link
   * ByteBuffer#position()} and {@link ByteBuffer#limit()} are updated by the users of this buffer
   * to perform the necessary operations. Note: {@link StoreIndexImpl} is not thread safe as defined
   * by {@link StoreIndex}
   */
  private final ByteBuffer serialized;

  /**
   * This buffer is used for temporary use within (de)serialization. Note: {@link StoreIndexImpl} is
   * not thread safe as defined by {@link StoreIndex}.
   */
  private final ByteBuffer scratchKeyBuffer = newKeyBuffer();

  private boolean modified;
  private ObjId objId;

  // NOTE: The implementation uses j.u.ArrayList to optimize for reads. Additions to this data
  // structure are rather inefficient, when elements need to be added "in the middle" of the
  // 'elements' j.u.ArrayList.

  StoreIndexImpl(ElementSerializer<V> serializer) {
    this(new ArrayList<>(), 2, serializer, false);
  }

  private StoreIndexImpl(
      List<StoreIndexElement<V>> elements,
      int originalSerializedSize,
      ElementSerializer<V> serializer,
      boolean modified) {
    this.elements = elements;
    this.originalSerializedSize = originalSerializedSize;
    this.serializer = serializer;
    this.modified = modified;
    this.serialized = null;
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  @VisibleForTesting
  StoreIndexImpl<V> setModified() {
    modified = true;
    return this;
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
    ElementSerializer<V> serializer = this.serializer;
    int size = e.size();
    for (int i = 0; i < size; i++) {
      StoreIndexElement<V> el = e.get(i);
      V updated = updater.apply(el);
      if (updated != el) {
        modified = true;
        int oldSerializedSize = el.contentSerializedSize(serializer);
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
  public boolean add(@Nonnull StoreIndexElement<V> element) {
    modified = true;
    List<StoreIndexElement<V>> e = elements;
    ElementSerializer<V> serializer = this.serializer;
    int idx = search(e, element);
    int elementSerializedSize = element.contentSerializedSize(serializer);
    if (idx >= 0) {
      // exact match, key already in segment
      StoreIndexElement<V> prev = e.get(idx);

      int prevSerializedSize = prev.contentSerializedSize(serializer);
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
  public boolean remove(@Nonnull StoreKey key) {
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
    return 2 + element.contentSerializedSize(serializer);
  }

  @Override
  public boolean contains(@Nonnull StoreKey key) {
    int idx = search(elements, key);
    return idx >= 0;
  }

  @Override
  public @Nullable StoreIndexElement<V> get(@Nonnull StoreKey key) {
    List<StoreIndexElement<V>> e = elements;
    int idx = search(e, key);
    if (idx < 0) {
      return null;
    }
    return e.get(idx);
  }

  @Nullable
  @Override
  public StoreKey first() {
    List<StoreIndexElement<V>> e = elements;
    return e.isEmpty() ? null : e.get(0).key();
  }

  @Nullable
  @Override
  public StoreKey last() {
    List<StoreIndexElement<V>> e = elements;
    return e.isEmpty() ? null : e.get(e.size() - 1).key();
  }

  @Override
  public @Nonnull Iterator<StoreIndexElement<V>> iterator(
      @Nullable StoreKey begin, @Nullable StoreKey end, boolean prefetch) {
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
        ? new AbstractIterator<>() {

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
    return new AbstractList<>() {
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
    return originalSerializedSize + estimatedSerializedSizeDiff;
  }

  @Override
  public @Nonnull ByteString serialize() {
    ByteBuffer target;

    if (serialized == null || modified) {
      target = ByteBuffer.allocate(estimatedSerializedSize());

      // Serialized segment index version
      if (SERIALIZE_VERSION >= CURRENT_STORE_INDEX_VERSION) {
        target.put((byte) 2);
        putVarInt(target, elementCount());
      } else {
        target.put((byte) 1);
      }

      ByteBuffer previousKey = null;

      @SuppressWarnings("UnnecessaryLocalVariable")
      ElementSerializer<V> ser = serializer;

      List<StoreIndexElement<V>> elements = this.elements;
      boolean onlyLazy;
      StoreIndexElement<V> previous = null;
      for (StoreIndexElement<V> el : elements) {
        ByteBuffer keyBuf = null;
        if (el.getClass() == LazyStoreIndexElement.class) {
          LazyStoreIndexElement lazyEl = (LazyStoreIndexElement) el;
          // The purpose of this 'if'-branch is to determine whether it can serialize the 'StoreKey'
          // by _not_ fully materializing the  `StoreKey`. This is possible if (and only if!) the
          // current and the previous element are `LazyStoreIndexElement`s, where the previous
          // element is exactly the one that has been deserialized.
          //noinspection RedundantIfStatement
          if (lazyEl.prefixLen == 0 || lazyEl.previous == previous) {
            // Can use the optimized serialization in `LazyStoreIndexElement`, if the current
            // element has no prefix of if the previously serialized element was also a
            // `LazyStoreIndexElement`. In other words: no intermediate `LazyStoreIndexElement` has
            // been removed and no new element has been added.
            onlyLazy = true;
          } else {
            // This if-branch detects whether an element has been removed from the index. In that
            // case serialization has to materialize the `StoreKey` for serialization.
            onlyLazy = false;
          }
          if (onlyLazy) {
            // Key serialization via 'LazyStoreIndexElement' is much cheaper (CPU and heap) than
            // having to first materialize and then serialize it.
            keyBuf = lazyEl.serializeKey(scratchKeyBuffer, previousKey);
          }
        } else {
          onlyLazy = false;
        }

        if (!onlyLazy) {
          // Either 'el' is not a 'LazyStoreIndexElement' or the previous element of a
          // 'LazyStoreIndexElement' is not suitable (see above).
          keyBuf = el.key().serialize(scratchKeyBuffer);
        }

        previousKey = serializeKey(keyBuf, previousKey, target);
        el.serializeContent(ser, target);
        previous = el;
      }

      target.flip();
    } else {
      target = serialized.position(0).limit(originalSerializedSize);
    }

    return unsafeWrap(target);
  }

  private ByteBuffer serializeKey(ByteBuffer keyBuf, ByteBuffer previousKey, ByteBuffer target) {
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
    previousKey.put(keyBuf).flip();

    return previousKey;
  }

  static <V> StoreIndex<V> deserializeStoreIndex(ByteBuffer serialized, ElementSerializer<V> ser) {
    return new StoreIndexImpl<>(serialized, ser);
  }

  /**
   * Private constructor handling deserialization, required to instantiate the inner {@link
   * LazyStoreIndexElement} class.
   */
  private StoreIndexImpl(ByteBuffer serialized, ElementSerializer<V> ser) {
    byte version = serialized.get();
    checkArgument(
        version == 1 || version == 2, "Unsupported serialized representation of KeyIndexSegment");

    List<StoreIndexElement<V>> elements =
        version >= 2 ? new ArrayList<>(readVarInt(serialized)) : new ArrayList<>();

    boolean first = true;
    int previousKeyLen = 0;
    LazyStoreIndexElement predecessor = null;
    LazyStoreIndexElement previous = null;

    while (serialized.remaining() > 0) {
      int strip = first ? 0 : readVarInt(serialized);
      first = false;

      int prefixLen = previousKeyLen - strip;
      int keyOffset = serialized.position();
      skipKey(serialized); // skip key
      int valueOffset = serialized.position();
      ser.skip(serialized); // skip content/value
      int endOffset = serialized.position();

      int keyPartLen = valueOffset - keyOffset;
      int totalKeyLen = prefixLen + keyPartLen;

      predecessor = cutPredecessor(predecessor, prefixLen, previous);

      // 'prefixLen==0' means that the current key represents the "full" key.
      // It has no predecessor that would be needed to re-construct (aka materialize) the full key.
      LazyStoreIndexElement elementPredecessor = prefixLen > 0 ? predecessor : null;
      LazyStoreIndexElement element =
          new LazyStoreIndexElement(
              elementPredecessor, previous, keyOffset, prefixLen, valueOffset, endOffset);
      if (elementPredecessor == null) {
        predecessor = element;
      } else if (predecessor.prefixLen > prefixLen) {
        predecessor = element;
      }
      elements.add(element);

      previous = element;
      previousKeyLen = totalKeyLen;
    }

    this.elements = elements;
    this.serializer = ser;
    this.serialized = serialized.duplicate().clear();
    this.originalSerializedSize = serialized.position();
  }

  /**
   * Identifies the earliest suitable predecessor, which is a very important step during
   * deserialization, because otherwise the chain of predecessors (to the element having a {@code
   * prefixLen==0}) can easily become very long in the order of many thousands "hops", which makes
   * key materialization overly expensive.
   */
  private LazyStoreIndexElement cutPredecessor(
      LazyStoreIndexElement predecessor, int prefixLen, LazyStoreIndexElement previous) {
    if (predecessor != null) {
      if (predecessor.prefixLen < prefixLen) {
        // If the current element's prefixLen is higher, let the current element's predecessor point
        // to the previous element.
        predecessor = previous;
      } else {
        // Otherwise find the predecessor that has "enough" data. Without this step, the chain of
        // predecessors would become extremely long.
        for (LazyStoreIndexElement p = predecessor; ; p = p.predecessor) {
          if (p == null || p.prefixLen < prefixLen) {
            break;
          }
          predecessor = p;
        }
      }
    }
    return predecessor;
  }

  private final class LazyStoreIndexElement extends AbstractStoreIndexElement<V> {
    /**
     * Points to the predecessor (in index order) that has a required part of the store-key needed
     * to deserialize. In other words, if multiple index-elements have the same {@code prefixLen},
     * this one points to the first one (in index order), because referencing the "intermediate"
     * predecessors in-between would yield no part of the store-key to be re-constructed.
     *
     * <p>This fields holds the "earliest" predecessor in deserialization order, as determined by
     * {@link #cutPredecessor(LazyStoreIndexElement, int, LazyStoreIndexElement)}.
     *
     * <p>Example:<code><pre>
     *  IndexElement #0 { prefixLen = 0, key = "aaa", predecessor = null }
     *  IndexElement #1 { prefixLen = 2, key = "aab", predecessor = #0 }
     *  IndexElement #2 { prefixLen = 2, key = "aac", predecessor = #0 }
     *  IndexElement #3 { prefixLen = 1, key = "abb", predecessor = #0 }
     *  IndexElement #4 { prefixLen = 0, key = "bbb", predecessor = null }
     *  IndexElement #5 { prefixLen = 2, key = "bbc", predecessor = #4 }
     *  IndexElement #6 { prefixLen = 3, key = "bbcaaa", predecessor = #5 }
     * </pre></code>
     */
    final LazyStoreIndexElement predecessor;

    /**
     * The previous element in the order of deserialization. This is needed later during
     * serialization.
     */
    final LazyStoreIndexElement previous;

    /** Number of bytes for this element's key that are held by its predecessor(s). */
    final int prefixLen;

    /**
     * Position in {@link StoreIndexImpl#serialized} at which this index-element's key part starts.
     */
    final int keyOffset;

    /** Position in {@link StoreIndexImpl#serialized} at which this index-element's value starts. */
    final int valueOffset;

    /**
     * Position in {@link #serialized} pointing to the first byte <em>after</em> this element's key
     * and value.
     */
    final int endOffset;

    /** The materialized key or {@code null}. */
    private StoreKey key;

    /** The materialized content or {@code null}. */
    private V content;

    LazyStoreIndexElement(
        LazyStoreIndexElement predecessor,
        LazyStoreIndexElement previous,
        int keyOffset,
        int prefixLen,
        int valueOffset,
        int endOffset) {
      this.predecessor = predecessor;
      this.previous = previous;
      this.keyOffset = keyOffset;
      this.prefixLen = prefixLen;
      this.valueOffset = valueOffset;
      this.endOffset = endOffset;
    }

    ByteBuffer serializeKey(ByteBuffer keySerBuffer, ByteBuffer previousKey) {
      keySerBuffer.clear();
      if (previousKey != null) {
        int limitSave = previousKey.limit();
        keySerBuffer.put(previousKey.limit(prefixLen).position(0));
        previousKey.limit(limitSave).position(0);
      }

      StoreIndexImpl<V> index = StoreIndexImpl.this;
      ByteBuffer serialized = requireNonNull(index.serialized);
      return keySerBuffer.put(serialized.limit(valueOffset).position(keyOffset)).flip();
    }

    private StoreKey materializeKey() {
      StoreIndexImpl<V> index = StoreIndexImpl.this;
      ByteBuffer serialized = requireNonNull(index.serialized);

      ByteBuffer suffix = serialized.limit(valueOffset).position(keyOffset);

      ByteBuffer keyBuffer;
      int preLen = prefixLen;
      if (preLen > 0) {
        keyBuffer = prefixKey(serialized, this, preLen).position(preLen).put(suffix).flip();
      } else {
        keyBuffer = suffix;
      }
      return deserializeKey(keyBuffer);
    }

    private ByteBuffer prefixKey(ByteBuffer serialized, LazyStoreIndexElement me, int remaining) {
      ByteBuffer keyBuffer = StoreIndexImpl.this.scratchKeyBuffer.clear();

      // This loop could be easier written using recursion. However, recursion is way more expensive
      // than this loop. Since this code is on a very hot code path, it is worth it.
      for (LazyStoreIndexElement e = me.predecessor; e != null; e = e.predecessor) {
        if (e.key != null) {
          // In case the current 'e' has its key already materialized, use that one to construct the
          // prefix for "our" key.
          int limitSave = keyBuffer.limit();
          try {
            // Call 'putString' with the parameter 'shortened==true' to instruct the function to
            // expect buffer overruns and handle those gracefully.
            StoreKey.putString(keyBuffer.limit(remaining).position(0), e.key.rawString(), true);
          } finally {
            keyBuffer.limit(limitSave);
          }
          break;
        }

        int prefixLen = e.prefixLen;
        int take = remaining - prefixLen;
        if (take > 0) {
          remaining -= take;

          for (int src = e.keyOffset, dst = e.prefixLen; take-- > 0; src++, dst++) {
            keyBuffer.put(dst, serialized.get(src));
          }
        }
      }

      return keyBuffer;
    }

    private ByteBuffer serializedForContent() {
      StoreIndexImpl<V> index = StoreIndexImpl.this;
      ByteBuffer serialized = requireNonNull(index.serialized);
      return serialized.limit(endOffset).position(valueOffset);
    }

    private V materializeContent() {
      return serializer.deserialize(serializedForContent());
    }

    @Override
    public void serializeContent(ElementSerializer<V> ser, ByteBuffer target) {
      target.put(serializedForContent());
    }

    @Override
    public int contentSerializedSize(ElementSerializer<V> ser) {
      return endOffset - valueOffset;
    }

    @Override
    public StoreKey key() {
      StoreKey k = key;
      if (k == null) {
        k = key = materializeKey();
      }
      return k;
    }

    @Override
    public V content() {
      V c = content;
      if (c == null) {
        c = content = materializeContent();
      }
      return c;
    }

    @Override
    public String toString() {
      StoreKey k = key;
      V c = content;
      if (k != null && c != null) {
        return super.toString();
      }

      StringBuilder sb = new StringBuilder("LazyStoreIndexElement(");
      if (k != null) {
        sb.append("key=").append(k);
      } else {
        sb.append("keyOffset=").append(keyOffset).append(", prefixLen=").append(prefixLen);
      }

      if (c != null) {
        sb.append(", content=").append(c);
      } else {
        sb.append(", valueOffset=").append(valueOffset).append(" endOffset=").append(endOffset);
      }

      return sb.toString();
    }
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

  private static <V> int search(List<StoreIndexElement<V>> e, @Nonnull StoreKey key) {
    // Need a StoreIndexElement for the sake of 'binarySearch()' (the content value isn't used)
    return search(e, indexElement(key, ""));
  }

  private static <V> int search(List<StoreIndexElement<V>> e, StoreIndexElement<?> element) {
    return binarySearch(e, element, KEY_COMPARATOR);
  }
}
