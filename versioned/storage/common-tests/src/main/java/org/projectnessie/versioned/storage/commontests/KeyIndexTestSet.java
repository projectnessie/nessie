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
package org.projectnessie.versioned.storage.commontests;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.lang.Math.pow;
import static java.util.UUID.randomUUID;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.deserializeStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.indexes.ElementSerializer;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;

/** Parameterized {@link StoreKey}-index test set. */
@Value.Immutable
public interface KeyIndexTestSet<ELEMENT> {

  static KeyIndexTestSet<CommitOp> basicIndexTestSet() {
    return KeyIndexTestSet.<CommitOp>newGenerator()
        .elementSupplier(key -> indexElement(key, commitOp(ADD, 1, randomObjId())))
        .elementSerializer(COMMIT_OP_SERIALIZER)
        .build()
        .generateIndexTestSet();
  }

  @Value.Parameter(order = 1)
  List<StoreKey> keys();

  @Value.Parameter(order = 2)
  ByteString serialized();

  @Value.Parameter(order = 3)
  StoreIndex<ELEMENT> keyIndex();

  @Value.Parameter(order = 4)
  StoreIndex<ELEMENT> sourceKeyIndex();

  static <ELEMENT> KeyIndexTestSet<ELEMENT> of(
      List<StoreKey> keys,
      ByteString serialized,
      StoreIndex<ELEMENT> keyIndex,
      StoreIndex<ELEMENT> sourceKeyIndex) {
    return ImmutableKeyIndexTestSet.of(keys, serialized, keyIndex, sourceKeyIndex);
  }

  static <ELEMENT> ImmutableIndexTestSetGenerator.Builder<ELEMENT> newGenerator() {
    return ImmutableIndexTestSetGenerator.builder();
  }

  @FunctionalInterface
  interface KeySet {
    List<StoreKey> keys();
  }

  /**
   * Generates {@link StoreKey}s consisting of a single element from the string representation of
   * random UUIDs.
   */
  @Value.Immutable
  abstract class RandomUuidKeySet implements KeySet {
    @Value.Default
    public int numKeys() {
      return 1000;
    }

    @Override
    public List<StoreKey> keys() {
      Set<StoreKey> keys = new TreeSet<>();
      for (int i = 0; i < numKeys(); i++) {
        keys.add(key(randomUUID().toString()));
      }
      return new ArrayList<>(keys);
    }
  }

  /**
   * Generates {@link StoreKey}s based on realistic name patterns using a configurable amount of
   * namespace levels, namespaces per level and tables per namespace. Key elements are derived from
   * a set of more than 80000 words, each at least 10 characters long. The {@link #deterministic()}
   * flag specifies whether the words are chosen deterministically.
   */
  @Value.Immutable
  abstract class RealisticKeySet implements KeySet {
    @Value.Default
    public int namespaceLevels() {
      return 1;
    }

    @Value.Default
    public int foldersPerLevel() {
      return 5;
    }

    @Value.Default
    public int tablesPerNamespace() {
      return 20;
    }

    @Value.Default
    public boolean deterministic() {
      return true;
    }

    @Override
    public List<StoreKey> keys() {
      // This is the fastest way to generate a ton of keys, tested using profiling/JMH.
      int namespacesFolders = (int) pow(namespaceLevels(), foldersPerLevel());
      Set<StoreKey> namespaces =
          newHashSetWithExpectedSize(
              namespacesFolders); // actual value is higher, but that's fine here
      Set<StoreKey> keys = new TreeSet<>();

      generateKeys(null, 0, namespaces, keys);

      return new ArrayList<>(keys);
    }

    private void generateKeys(
        StoreKey current, int level, Set<StoreKey> namespaces, Set<StoreKey> keys) {
      if (level > namespaceLevels()) {
        return;
      }

      if (level == namespaceLevels()) {
        // generate tables
        for (int i = 0; i < tablesPerNamespace(); i++) {
          generateTableKey(current, level, keys, i);
        }
        return;
      }

      for (int i = 0; i < foldersPerLevel(); i++) {
        StoreKey folderKey = generateFolderKey(current, level, namespaces, i);
        generateKeys(folderKey, level + 1, namespaces, keys);
      }
    }

    private void generateTableKey(StoreKey current, int level, Set<StoreKey> keys, int i) {
      if (deterministic()) {
        StoreKey tableKey = keyFromString(current.rawString() + "\u0000" + Words.WORDS.get(i));
        checkArgument(keys.add(tableKey), "table - current:%s level:%s i:%s", current, level, i);
      } else {
        while (true) {
          StoreKey tableKey = keyFromString(current.rawString() + "\u0000" + randomWord());
          if (keys.add(tableKey)) {
            break;
          }
        }
      }
    }

    private StoreKey generateFolderKey(
        StoreKey current, int level, Set<StoreKey> namespaces, int i) {
      if (deterministic()) {
        String folder = Words.WORDS.get(i);
        StoreKey folderKey =
            current != null ? keyFromString(current.rawString() + "\u0000" + folder) : key(folder);
        checkArgument(
            namespaces.add(folderKey), "namespace - current:%s level:%s i:%s", current, level, i);
        return folderKey;
      } else {
        while (true) {
          String folder = randomWord();
          StoreKey folderKey =
              current != null
                  ? keyFromString(current.rawString() + "\u0000" + folder)
                  : key(folder);
          if (namespaces.add(folderKey)) {
            return folderKey;
          }
        }
      }
    }
  }

  @Value.Immutable
  abstract class IndexTestSetGenerator<ELEMENT> {

    public abstract Function<StoreKey, StoreIndexElement<ELEMENT>> elementSupplier();

    public abstract ElementSerializer<ELEMENT> elementSerializer();

    @Value.Default
    public KeySet keySet() {
      return ImmutableRealisticKeySet.builder().build();
    }

    public final KeyIndexTestSet<ELEMENT> generateIndexTestSet() {
      StoreIndex<ELEMENT> index = newStoreIndex(elementSerializer());

      List<StoreKey> keys = keySet().keys();

      for (StoreKey key : keys) {
        index.add(elementSupplier().apply(key));
      }

      ByteString serialized = index.serialize();

      // Re-serialize to have "clean" internal values in KeyIndexImpl
      StoreIndex<ELEMENT> keyIndex = deserializeStoreIndex(serialized, elementSerializer());

      return KeyIndexTestSet.of(keys, keyIndex.serialize(), keyIndex, index);
    }
  }

  static String randomWord() {
    return Words.WORDS.get(ThreadLocalRandom.current().nextInt(Words.WORDS.size()));
  }

  default StoreKey randomKey() {
    List<StoreKey> k = keys();
    int i = ThreadLocalRandom.current().nextInt(k.size());
    return k.get(i);
  }

  default ByteString serialize() {
    return keyIndex().serialize();
  }

  default StoreIndex<CommitOp> deserialize() {
    return deserializeStoreIndex(serialized(), COMMIT_OP_SERIALIZER);
  }

  default StoreIndexElement<ELEMENT> randomGetKey() {
    StoreKey key = randomKey();
    return keyIndex().get(key);
  }

  class Words {
    private static final List<String> WORDS = new ArrayList<>();

    static {
      // Word list "generated" via:
      //
      // curl https://raw.githubusercontent.com/sindresorhus/word-list/main/words.txt |
      // while read word; do
      //   [[ ${#word} -gt 10 ]] && echo $word
      // done | gzip > words.gz
      //
      try {
        URL words = KeyIndexTestSet.class.getResource("words.gz");
        URLConnection conn =
            Objects.requireNonNull(words, "words.gz resource not found").openConnection();
        try (BufferedReader br =
            new BufferedReader(
                new InputStreamReader(
                    new GZIPInputStream(conn.getInputStream()), StandardCharsets.UTF_8))) {
          String line;
          while ((line = br.readLine()) != null) {
            WORDS.add(line);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
