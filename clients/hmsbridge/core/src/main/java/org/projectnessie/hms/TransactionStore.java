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
package org.projectnessie.hms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.ImmutableEntry;
import org.projectnessie.model.ImmutableMultiGetContentsRequest;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

/**
 * Interface that guarantees consistency of access to underlying data. Caches all operations within a commit and makes
 * sure all rest api operations are merged with locally cached but not yet committed changes.
 */
class TransactionStore {

  private final Reference reference;
  private final TreeApi tree;
  private final ContentsApi contents;
  private final Map<RefKey, Item> cachedItems = new HashMap<>();
  private final List<Operation> operations = new ArrayList<>();
  private final boolean writable;

  public TransactionStore(Reference reference, ContentsApi contents, TreeApi tree) {
    this.contents = contents;
    this.reference = reference;
    this.writable = reference instanceof Branch;
    this.tree = tree;
  }

  Optional<Item> getItemForRef(String ref, ContentsKey contentsKey) throws NoSuchObjectException {

    RefKey key = new RefKey(ref, contentsKey);
    if (cachedItems.containsKey(key)) {
      // could be null but the cache is the up to date/operation mapped situation.
      return Optional.ofNullable(cachedItems.get(key));
    }

    try {
      Item item = Item.fromContents(contents.getContents(contentsKey, ref));
      cachedItems.put(key,  item);
      return Optional.ofNullable(item);
    } catch (NessieNotFoundException e) {
      throw new NoSuchObjectException(String.format("Unable to find ref [%s].", ref));
    }
  }

  List<Optional<Item>> getItemsForRef(List<RefKey> refKeys) throws NessieNotFoundException {
    List<Optional<Item>> items = new ArrayList<>();
    Map<RefKey, Integer> keyToPos = new HashMap<>();
    List<RefKey> refKeysToRetrieve = new ArrayList<>();
    for (int i = 0; i < refKeys.size(); i++) {
      RefKey key = refKeys.get(i);
      keyToPos.put(key, i);
      if (cachedItems.containsKey(key)) {
        // may be a null pointer (deleted item)
        items.set(i, Optional.ofNullable(cachedItems.get(key)));
      } else {
        items.set(i, Optional.empty());
        refKeysToRetrieve.add(key);
      }
    }

    ListMultimap<String, RefKey> keysByRef = Multimaps.index(refKeysToRetrieve, RefKey::getRef);

    for (String ref : keysByRef.keySet()) {
      List<ContentsKey> keys = keysByRef.get(ref).stream().map(RefKey::getKey).collect(Collectors.toList());
      MultiGetContentsResponse response = contents.getMultipleContents(ref,
          ImmutableMultiGetContentsRequest.builder().addAllRequestedKeys(keys).build());
      response.getContents().forEach(cwk -> {
        RefKey key = new RefKey(ref, cwk.getKey());
        items.set(keyToPos.get(key), Optional.of(Item.fromContents(cwk.getContents())));
      });
    }

    return items;
  }

  public List<Reference> getReferences() {
    return tree.getAllReferences();
  }

  public Stream<Entry> getEntriesForDefaultRef() throws NessieNotFoundException {
    List<Entry> entries = tree.getEntries(reference.getHash(), null, null).getEntries();
    Supplier<Stream<RefKey>> defaultRefKeys = () -> cachedItems.keySet().stream().filter(k -> k.getRef().equals(reference.getHash()));
    Set<ContentsKey> toRemove = defaultRefKeys.get().map(RefKey::getKey).collect(Collectors.toSet());
    return Stream.concat(entries.stream().filter(k -> !toRemove.contains(k.getName())),
        defaultRefKeys.get().map(r -> ImmutableEntry.builder().name(r.getKey()).type(Type.UNKNOWN).build()));
  }

  void setItem(ContentsKey key, Item item) {
    checkWritable();
    cachedItems.put(new RefKey(reference.getHash(),  key), item);
    operations.add(Operation.Put.of(key, item.toContents()));
  }

  void commit() throws NessieNotFoundException, NessieConflictException {
    if (operations.isEmpty()) {
      return;
    }

    checkWritable();
    tree.commitMultipleOperations(reference.getName(), reference.getHash(), "HMS commit",
        ImmutableOperations.builder().addAllOperations(operations.stream().collect(Collectors.toList())).build());
  }

  void deleteItem(ContentsKey key) {
    checkWritable();
    cachedItems.put(new RefKey(reference.getHash(),  key), null);
    operations.add(Operation.Delete.of(key));
  }

  private void checkWritable() {
    if (!writable) {
      throw new IllegalArgumentException("Changes can only be applied to branches. The provided ref is not a branch.");
    }
  }

  public static class RefKey {
    private final String ref;
    private final ContentsKey key;

    public RefKey(String ref, ContentsKey key) {
      super();
      this.ref = ref;
      this.key = key;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, ref);
    }

    public String getRef() {
      return ref;
    }

    public ContentsKey getKey() {
      return key;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RefKey)) {
        return false;
      }
      RefKey other = (RefKey) obj;
      return Objects.equals(key, other.key) && Objects.equals(ref, other.ref);
    }

  }
}
