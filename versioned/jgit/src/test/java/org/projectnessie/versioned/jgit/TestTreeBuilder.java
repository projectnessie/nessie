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
package org.projectnessie.versioned.jgit;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.WithEntityType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;

class TestTreeBuilder {


  @Test
  void test() throws IOException {
    Repository repository;
    ObjectId emptyObject;
    try {
      repository = new InMemoryRepository.Builder().setRepositoryDescription(new DfsRepositoryDescription()).build();
      ObjectInserter oi = repository.newObjectInserter();
      emptyObject = oi.insert(Constants.OBJ_BLOB, new byte[] {0});
      oi.flush();
    } catch (IOException e) {
      throw new RuntimeException();
    }
    Serializer<WithEntityType<String>> serializer = new Serializer<WithEntityType<String>>() {
      @Override
      public ByteString toBytes(WithEntityType<String> value) {
        return ByteString.copyFrom(value.getValue().getBytes());
      }

      @Override
      public WithEntityType<String> fromBytes(ByteString bytes) {
        return WithEntityType.of((byte)0, bytes.toStringUtf8());
      }
    };
    ObjectId oid1 = TreeBuilder.commitObjects(ImmutableList.of(Put.of(Key.of("a", "b", "c.txt"), entityType("foobar")),
                                                               Put.of(Key.of("a", "b", "d.txt"), entityType("foobar")),
                                                               Put.of(Key.of("a", "i", "j.txt"), entityType("foobar")),
                                                               Put.of(Key.of("a", "f.txt"), entityType("foobar1")),
                                                               Put.of(Key.of("k", "l.txt"), entityType("foobar"))),
                                              repository, serializer, emptyObject);
    ObjectId oid2 = TreeBuilder.commitObjects(ImmutableList.of(Put.of(Key.of("a", "g", "h.txt"), entityType("foobar")),
                                                               Delete.of(Key.of("a", "b", "d.txt")),
                                                               Put.of(Key.of("a", "f.txt"), entityType("foobar")),
                                                               Put.of(Key.of("a","b","p.txt"), entityType("foobar")),
                                                               Put.of(Key.of("m", "n", "o.txt"), entityType("foobar")),
                                                               Unchanged.of(Key.of("a","b","c.txt"))),
                                              repository, serializer, emptyObject);

    ObjectId oid3 = TreeBuilder.merge(oid1, oid2, repository);
    TreeWalk tw = new TreeWalk(repository);
    tw.addTree(oid3);
    tw.setRecursive(true);
    Map<String, String> results = new HashMap<>();
    Set<String> expected = ImmutableSet.of("a/b/c.txt", "a/g/h.txt", "a/f.txt", "a/i/j.txt", "k/l.txt", "m/n/o.txt");
    while (tw.next()) {
      if (ObjectId.zeroId().equals(tw.getObjectId(0))) {
        continue;
      }
      WithEntityType<String> value = serializer.fromBytes(
          ByteString.copyFrom(repository.newObjectReader().open(tw.getObjectId(0)).getBytes()));
      results.put(tw.getPathString(), value.getValue());
    }
    Assertions.assertTrue(Sets.difference(expected, results.keySet()).isEmpty());
    Set<String> valueSet = new HashSet<>(results.values());
    Assertions.assertEquals(1, valueSet.size());
    String value = valueSet.iterator().next();
    Assertions.assertEquals("foobar", value);
  }

  private WithEntityType<String> entityType(String foobar) {
    return WithEntityType.of((byte)0, foobar);
  }
}
