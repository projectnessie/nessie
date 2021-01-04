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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.versioned.impl.InternalBranch.Commit;
import com.dremio.nessie.versioned.impl.InternalBranch.UnsavedDelta;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.stream.Stream;

/**
 * Generic class for reading a reference.
 */
public interface InternalRef extends HasId {

  static final String TYPE = "type";

  public static enum Type {
    BRANCH("b"),
    TAG("t"),
    HASH(null),
    UNKNOWN(null);

    private final Entity value;

    Type(String identifier) {
      this.value = Entity.ofString(identifier);
    }

    public ExpressionFunction typeVerification() {
      return ExpressionFunction.equals(ExpressionPath.builder(TYPE).build(), toEntity());
    }

    /**
     * Convert the type to it's entity type tag.
     * @return A Entity holding the type tag.
     */
    public Entity toEntity() {
      if (this == HASH) {
        throw new IllegalStateException("You should not try to retrieve the identifier for a hash "
            + "type since they are not saveable as searchable refs.");
      }
      return value;
    }

    /**
     * Get the type associated with this type tag.
     * @param identifier The type tag to classify.
     * @return The type classified.
     */
    public static Type getType(String identifier) {
      if (identifier.equals("b")) {
        return BRANCH;
      } else if (identifier.equals("t")) {
        return TAG;
      } else {
        throw new IllegalArgumentException(String.format("Unknown identifier name [%s].", identifier));
      }
    }
  }

  Type getType();

  default InternalBranch getBranch() {
    throw new IllegalArgumentException(String.format("%s cannot be treated as a branch.", this.getClass().getName()));
  }

  default InternalTag getTag() {
    throw new IllegalArgumentException(String.format("%s cannot be treated as a tag.", this.getClass().getName()));
  }

  default Id getHash() {
    throw new IllegalArgumentException(String.format("%s cannot be treated as a hash.", this.getClass().getName()));
  }

  Id getId();

  static final SimpleSchema<InternalRef> SCHEMA = new SimpleSchema<InternalRef>(InternalRef.class) {

    @Override
    public InternalRef deserialize(Map<String, Entity> attributeMap) {
      Type type = Type.getType(attributeMap.get(TYPE).getString());
      Map<String, Entity> filtered = Maps.filterEntries(attributeMap, e -> !e.getKey().equals(TYPE));
      switch (type) {
        case BRANCH: return InternalBranch.SCHEMA.mapToItem(filtered);
        case TAG: return InternalTag.SCHEMA.mapToItem(filtered);
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    public Map<String, Entity> itemToMap(InternalRef item, boolean ignoreNulls) {
      Map<String, Entity> map = new HashMap<>();
      map.put(TYPE, item.getType().toEntity());

      switch (item.getType()) {
        case BRANCH:
          map.putAll(InternalBranch.SCHEMA.itemToMap(item.getBranch(), ignoreNulls));
          break;
        case TAG:
          map.putAll(InternalTag.SCHEMA.itemToMap(item.getTag(), ignoreNulls));
          break;
        default:
          throw new UnsupportedOperationException();
      }

      return map;
    }

  };

  static Builder builder() {
    return new Builder();
  }

  class Builder implements RefConsumer<InternalRef.Builder> {

    private Id id;
    private RefType refType;
    private String name;

    // tag only
    private Id commit;

    // branch only
    private Id metadata;
    private Stream<Id> children;
    private Stream<BranchCommit> commits;

    @Override
    public Builder id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return this;
    }

    @Override
    public Builder type(RefType refType) {
      checkCalled(this.refType, "refType");
      this.refType = refType;
      return this;
    }

    @Override
    public Builder name(String name) {
      checkCalled(this.name, "name");
      this.name = name;
      return this;
    }

    @Override
    public Builder commit(Id commit) {
      checkCalled(this.commit, "commit");
      this.commit = commit;
      return this;
    }

    @Override
    public Builder metadata(Id metadata) {
      checkCalled(this.metadata, "metadata");
      this.metadata = metadata;
      return this;
    }

    @Override
    public Builder children(Stream<Id> children) {
      checkCalled(this.children, "children");
      this.children = children;
      return this;
    }

    @Override
    public Builder commits(Stream<BranchCommit> commits) {
      checkCalled(this.commits, "commits");
      this.commits = commits;
      return this;
    }

    public InternalRef build() {
      checkSet(id, "id");
      checkSet(refType, "refType");
      checkSet(name, "name");

      switch (refType) {
        case TAG:
          checkSet(commit, "commit");
          return new InternalTag(id, name, commit);
        case BRANCH:
          checkSet(metadata, "metadata");
          checkSet(children, "children");
          checkSet(commits, "commits");
          return new InternalBranch(
              id,
              name,
              IdMap.of(children),
              metadata,
              commits
                  .map(bc -> bc.isSaved()
                      ? new Commit(bc.getId(), bc.getCommit(), bc.getParent())
                      : new Commit(
                          bc.getId(),
                          bc.getCommit(),
                          bc.getDeltas().stream()
                            .map(d -> new UnsavedDelta(d.getPosition(), d.getOldId(), d.getNewId()))
                            .collect(Collectors.toList()),
                          keyMutations(bc)
                          ))
                  .collect(Collectors.toList()));
        default:
          throw new UnsupportedOperationException("Unknown ref-type " + refType);
      }
    }

    private KeyMutationList keyMutations(BranchCommit bc) {
      List<KeyMutation> mutations = new ArrayList<>();
      bc.getKeyAdditions().stream().map(km -> KeyMutation.KeyAddition.of(new InternalKey(km))).forEach(mutations::add);
      bc.getKeyRemovals().stream().map(km -> KeyMutation.KeyRemoval.of(new InternalKey(km))).forEach(mutations::add);
      return KeyMutationList.of(mutations);
    }

    private static void checkCalled(Object arg, String name) {
      Preconditions.checkArgument(arg == null, String.format("Cannot call %s more than once", name));
    }

    private static void checkSet(Object arg, String name) {
      Preconditions.checkArgument(arg != null, String.format("Must call %s", name));
    }
  }

}
