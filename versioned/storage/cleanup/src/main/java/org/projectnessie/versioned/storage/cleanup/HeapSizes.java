/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cleanup;

import static java.lang.String.format;

final class HeapSizes {
  private HeapSizes() {}

  /*
  org.agrona.collections.ObjectHashSet object internals:
  OFF  SZ                                                  TYPE DESCRIPTION                           VALUE
    0   8                                                       (object header: mark)                 N/A
    8   4                                                       (object header: class)                N/A
   12   4                                                 float ObjectHashSet.loadFactor              N/A
   16   4                                                   int ObjectHashSet.resizeThreshold         N/A
   20   4                                                   int ObjectHashSet.size                    N/A
   24   1                                               boolean ObjectHashSet.shouldAvoidAllocation   N/A
   25   7                                                       (alignment/padding gap)
   32   8                                    java.lang.Object[] ObjectHashSet.values                  N/A
   40   8   org.agrona.collections.ObjectHashSet.ObjectIterator ObjectHashSet.iterator                N/A
   48   8                        java.util.function.IntConsumer ObjectHashSet.resizeNotifier          N/A
   56   8                                                       (object alignment gap)
  Instance size: 64 bytes
  Space losses: 7 bytes internal + 8 bytes external = 15 bytes total
   */
  static final long HEAP_SIZE_OBJECT_HASH_SET = 64L;
  /*
  org.projectnessie.versioned.storage.common.persist.ObjId$ObjId256 object internals:
  OFF  SZ   TYPE DESCRIPTION               VALUE
    0   8        (object header: mark)     N/A
    8   4        (object header: class)    N/A
   12   4        (alignment/padding gap)
   16   8   long ObjId256.l0               N/A
   24   8   long ObjId256.l1               N/A
   32   8   long ObjId256.l2               N/A
   40   8   long ObjId256.l3               N/A
  Instance size: 48 bytes
  Space losses: 4 bytes internal + 0 bytes external = 4 bytes total
   */
  static final long HEAP_SIZE_OBJ_ID = 48L;
  /*
  long[] : 16 + 8*length
   */
  static final long HEAP_SIZE_PRIMITIVE_OBJ_ARRAY = 16L;

  /*
  com.google.common.hash.BloomFilter object internals:
  OFF  SZ                                                            TYPE DESCRIPTION                    VALUE
    0   8                                                                 (object header: mark)          N/A
    8   8                                                                 (object header: class)         N/A
   16   4                                                             int BloomFilter.numHashFunctions   N/A
   20   4                                                                 (alignment/padding gap)
   24   8   com.google.common.hash.BloomFilterStrategies.LockFreeBitArray BloomFilter.bits               N/A
   32   8                                   com.google.common.hash.Funnel BloomFilter.funnel             N/A
   40   8                     com.google.common.hash.BloomFilter.Strategy BloomFilter.strategy           N/A
  Instance size: 48 bytes
  Space losses: 4 bytes internal + 0 bytes external = 4 bytes total
   */
  static final long HEAP_SIZE_BLOOM_FILTER = 48L;
  /*
  com.google.common.hash.BloomFilterStrategies$LockFreeBitArray object internals:
  OFF  SZ                                          TYPE DESCRIPTION                 VALUE
    0   8                                               (object header: mark)       N/A
    8   8                                               (object header: class)      N/A
   16   8   java.util.concurrent.atomic.AtomicLongArray LockFreeBitArray.data       N/A
   24   8            com.google.common.hash.LongAddable LockFreeBitArray.bitCount   N/A
  Instance size: 32 bytes
  Space losses: 0 bytes internal + 0 bytes external = 0 bytes total
  */
  static final long HEAP_SIZE_BIT_ARRAY = 32L;
  /*
  We assume that com.google.common.hash.LongAddables uses the pure-Java implementation, not Guava's
  heap-expensive LongAdder implementation based on its Striped64 with 144 bytes per cell.

  java.util.concurrent.atomic.AtomicLong object internals (com.google.common.hash.LongAddables.PureJavaLongAddable):
  OFF  SZ   TYPE DESCRIPTION               VALUE
    0   8        (object header: mark)     N/A
    8   4        (object header: class)    N/A
   12   4        (alignment/padding gap)
   16   8   long AtomicLong.value          N/A
   24   8        (object alignment gap)
  Instance size: 32 bytes
  Space losses: 4 bytes internal + 8 bytes external = 12 bytes total
  */
  static final long HEAP_SIZE_LONG_ADDER = 40L;
  /*
  java.util.concurrent.atomic.AtomicLongArray object internals:
  OFF  SZ     TYPE DESCRIPTION               VALUE
    0   8          (object header: mark)     N/A
    8   4          (object header: class)    N/A
   12   4          (alignment/padding gap)
   16   8   long[] AtomicLongArray.array     N/A
   24   8          (object alignment gap)
  Instance size: 32 bytes
  Space losses: 4 bytes internal + 8 bytes external = 12 bytes total
  */
  static final long HEAP_SIZE_ATOMIC_LONG_ARRAY = 32L;
  /*
  long[] : 16 + 8*length
   */
  static final long HEAP_SIZE_PRIMITIVE_LONG_ARRAY = 16L;

  /*
  java.util.LinkedHashMap object internals:
  OFF  SZ                            TYPE DESCRIPTION                 VALUE
    0   8                                 (object header: mark)       N/A
    8   4                                 (object header: class)      N/A
   12   4                             int HashMap.size                N/A
   16   8                   java.util.Set AbstractMap.keySet          N/A
   24   8            java.util.Collection AbstractMap.values          N/A
   32   4                             int HashMap.modCount            N/A
   36   4                             int HashMap.threshold           N/A
   40   4                           float HashMap.loadFactor          N/A
   44   4                             int LinkedHashMap.putMode       N/A
   48   8        java.util.HashMap.Node[] HashMap.table               N/A
   56   8                   java.util.Set HashMap.entrySet            N/A
   64   1                         boolean LinkedHashMap.accessOrder   N/A
   65   7                                 (alignment/padding gap)
   72   8   java.util.LinkedHashMap.Entry LinkedHashMap.head          N/A
   80   8   java.util.LinkedHashMap.Entry LinkedHashMap.tail          N/A
   88   8                                 (object alignment gap)
  Instance size: 96 bytes
  Space losses: 7 bytes internal + 8 bytes external = 15 bytes total
   */
  static final long HEAP_SIZE_LINKED_HASH_MAP = 96L;
  /*
  java.util.LinkedHashMap$Entry object internals:
  OFF  SZ                            TYPE DESCRIPTION               VALUE
    0   8                                 (object header: mark)     N/A
    8   8                                 (object header: class)    N/A
   16   4                             int Node.hash                 N/A
   20   4                                 (alignment/padding gap)
   24   8                java.lang.Object Node.key                  N/A
   32   8                java.lang.Object Node.value                N/A
   40   8          java.util.HashMap.Node Node.next                 N/A
   48   8   java.util.LinkedHashMap.Entry Entry.before              N/A
   56   8   java.util.LinkedHashMap.Entry Entry.after               N/A
  Instance size: 64 bytes
  Space losses: 4 bytes internal + 0 bytes external = 4 bytes total
   */
  static final long HEAP_SIZE_LINKED_HASH_MAP_ENTRY = 64L;

  static final long HEAP_SIZE_POINTER = 8L;

  static String memSizeToStringMB(long bytes) {
    return format("%.1f M", ((double) bytes) / 1024L / 1024L);
  }
}
