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
package com.dremio.nessie.versioned.store.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.tiered.builder.base.AbstractRef.AbstractBranch;
import com.dremio.nessie.tiered.builder.base.AbstractRef.AbstractTag;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.Key.Mutation;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath.NameSegment;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath.PathSegment;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.jdbc.JdbcEntity.SQLChange;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.primitives.Ints;

class JdbcRef extends JdbcBaseValue<Ref> implements Ref {

  static final String TYPE = "type";
  static final String NAME = "name";
  static final String COMMIT = "commit";
  static final String REF_TYPE_BRANCH = "b";
  static final String REF_TYPE_TAG = "t";
  static final String METADATA = "metadata";

  static final String[] TREE_COLUMNS = IntStream.range(0, 43).mapToObj(i -> "tree_" + i).toArray(String[]::new);

  static final String C_ID = "c_id";
  static final String C_COMMIT = "c_commit";
  static final String C_PARENT = "c_parent";
  static final String C_DELTAS = "c_deltas";
  static final String C_KEY_LIST = "c_keys";

  static final String[] C_COLUMNS = new String[]{C_ID, C_COMMIT, C_PARENT, C_DELTAS, C_KEY_LIST};

  static final int MAX_COMMITS = 15;

  static JdbcEntity<Ref> createEntity(DatabaseAdapter databaseAdapter, JdbcStoreConfig config) {
    Builder<String, ColumnType> columns = JdbcBaseValue.columnMapBuilder()
        .put(TYPE, ColumnType.REF_TYPE)
        .put(NAME, ColumnType.REF_NAME)
        .put(COMMIT, ColumnType.ID)
        .put(METADATA, ColumnType.ID);
    for (String treeColumn : TREE_COLUMNS) {
      columns.put(treeColumn, ColumnType.ID);
    }

    for (int i = 0; i < MAX_COMMITS; i++) {
      String suffix = makeSuffix(i);
      columns.put(C_ID + suffix, ColumnType.ID)
          .put(C_COMMIT + suffix, ColumnType.ID)
          .put(C_PARENT + suffix, ColumnType.ID)
          .put(C_DELTAS + suffix, ColumnType.KEY_DELTA_LIST)
          .put(C_KEY_LIST + suffix, ColumnType.KEY_MUTATION_LIST);
    }

    return new JdbcEntity<>(databaseAdapter, ValueType.REF, config,
        columns.build(),
        JdbcRef::new,
        (r, c) -> produceToConsumer(databaseAdapter, r, c));
  }

  private static String makeSuffix(int i) {
    return "_" + i;
  }

  private static void produceToConsumer(DatabaseAdapter databaseAdapter, ResultSet resultSet, Ref consumer) throws SQLException {
    JdbcBaseValue.produceToConsumer(databaseAdapter, resultSet, consumer)
        .name(databaseAdapter.getString(resultSet, NAME));
    String type = databaseAdapter.getString(resultSet, TYPE);
    switch (type) {
      case REF_TYPE_BRANCH:
        consumer.branch()
            .metadata(databaseAdapter.getId(resultSet, METADATA))
            .children(Stream.of(TREE_COLUMNS).map(c -> {
              try {
                return databaseAdapter.getId(resultSet, c);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }).collect(Collectors.toList()).stream())
            .commits(bc -> {
              try {
                for (int i = 0; i < MAX_COMMITS; i++) {
                  String suffix = makeSuffix(i);

                  Id commitId = databaseAdapter.getId(resultSet, C_ID + suffix);
                  if (commitId == null) {
                    break;
                  }

                  bc.id(commitId)
                      .commit(databaseAdapter.getId(resultSet, C_COMMIT + suffix));
                  Id commitParent = databaseAdapter.getId(resultSet, C_PARENT + suffix);
                  if (commitParent != null) {
                    bc.saved().parent(commitParent).done();
                  } else {
                    UnsavedCommitDelta unsaved = bc.unsaved();
                    databaseAdapter.getUnsavedDeltas(resultSet, C_DELTAS + suffix, unsaved);
                    UnsavedCommitMutations mutations = unsaved.mutations();
                    databaseAdapter.getMutations(resultSet, C_KEY_LIST + suffix, mutations::keyMutation);
                    mutations.done();
                  }
                }
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            });
        break;
      case REF_TYPE_TAG:
        consumer.tag()
            .commit(databaseAdapter.getId(resultSet, COMMIT));
        break;
      default:
        throw new IllegalStateException("Unknown ref-type " + type);
    }
  }

  JdbcRef(Resources resources, SQLChange change, JdbcEntity<Ref> entity) {
    super(resources, change, entity);
  }

  @Override
  public Ref name(String name) {
    entity.databaseAdapter.setString(change, NAME, name);
    return this;
  }

  @Override
  public Tag tag() {
    entity.databaseAdapter.setString(change, TYPE, REF_TYPE_TAG);
    return new AbstractTag(this) {
      @Override
      public Tag commit(Id commit) {
        entity.databaseAdapter.setId(change, COMMIT, commit);
        return this;
      }
    };
  }

  @Override
  public Branch branch() {
    entity.databaseAdapter.setString(change, TYPE, REF_TYPE_BRANCH);
    return new AbstractBranch(this) {
      @Override
      public Branch metadata(Id metadata) {
        entity.databaseAdapter.setId(change, METADATA, metadata);
        return this;
      }

      @Override
      public Branch children(Stream<Id> children) {
        List<Id> ch = children.collect(Collectors.toList());
        if (ch.size() != TREE_COLUMNS.length) {
          throw new IllegalArgumentException("Expected " + TREE_COLUMNS.length + " ids, got " + ch.size());
        }
        for (int i = 0; i < TREE_COLUMNS.length; i++) {
          entity.databaseAdapter.setId(change, TREE_COLUMNS[i], ch.get(i));
        }
        return this;
      }

      @Override
      public Branch commits(Consumer<BranchCommit> commits) {
        commits.accept(new JdbcBranchCommit(0, change));
        return this;
      }
    };
  }

  class JdbcBranchCommit implements BranchCommit, SavedCommit, UnsavedCommitDelta,
      UnsavedCommitMutations {

    private final SQLChange bcChange;

    private List<String> deltas;
    private List<String> keyMutations;

    private String suffix;
    private int offset;

    JdbcBranchCommit(int offset, SQLChange bcChange) {
      this.offset = offset;
      this.suffix = makeSuffix(offset);
      this.bcChange = bcChange;
    }

    private void nextCommit() {
      offset++;
      this.suffix = makeSuffix(offset);
    }

    @Override
    public BranchCommit id(Id id) {
      entity.databaseAdapter.setId(bcChange, C_ID + suffix, id);
      return this;
    }

    @Override
    public BranchCommit commit(Id commit) {
      entity.databaseAdapter.setId(bcChange, C_COMMIT + suffix, commit);
      return this;
    }

    @Override
    public SavedCommit saved() {
      return this;
    }

    @Override
    public SavedCommit parent(Id parent) {
      entity.databaseAdapter.setId(bcChange, C_PARENT + suffix, parent);
      return this;
    }

    @Override
    public UnsavedCommitDelta unsaved() {
      entity.databaseAdapter.setId(bcChange, C_PARENT + suffix, null);
      return this;
    }

    @Override
    public UnsavedCommitDelta delta(int position, Id oldId, Id newId) {
      if (deltas == null) {
        deltas = new ArrayList<>();
      }
      deltas.add(Integer.toString(position) + ',' + oldId + ',' + newId);
      return this;
    }

    @Override
    public UnsavedCommitMutations mutations() {
      return this;
    }

    @Override
    public UnsavedCommitMutations keyMutation(Mutation keyMutation) {
      if (keyMutations == null) {
        keyMutations = new ArrayList<>();
      }
      keyMutations.add(DatabaseAdapter.mutationAsString(keyMutation));
      return this;
    }

    @Override
    public BranchCommit done() {
      entity.databaseAdapter.setStrings(bcChange, C_DELTAS + suffix, deltas, ColumnType.KEY_DELTA_LIST);
      entity.databaseAdapter.setStrings(bcChange, C_KEY_LIST + suffix, keyMutations, ColumnType.KEY_MUTATION_LIST);

      deltas = null;
      keyMutations = null;

      nextCommit();

      return this;
    }
  }

  @Override
  public void conditionSize(UpdateContext updateContext, Optional<ConditionExpression> condition,
      ExpressionPath path, int expectedSize) {
    if (!path.getRoot().getChild().isPresent()) {
      switch (path.getRoot().getName()) {
        case "commits":
          for (int i = 0; i < MAX_COMMITS; i++) {
            updateContext.change.addConditionExpression(C_ID + makeSuffix(i),
                C_ID + makeSuffix(i) + (i < expectedSize ? " IS NOT NULL" : " IS NULL"));
          }
          return;
        default:
          break;
      }
    }
    super.conditionSize(updateContext, condition, path, expectedSize);
  }

  @Override
  void updateListAppend(UpdateContext updateContext, ExpressionPath path, Entity value) {
    NameSegment root = path.getRoot();
    if (root.getChild().isPresent()) {
      throw new UnsupportedOperationException("list-append with child not supported " + path);
    }

    String rootName = root.getName();
    switch (rootName) {
      case "commits":
        // value is a list of maps
        int offset = nextCommitIndex(updateContext.id);
        offset += updateContext.adjustedIndex(rootName);
        updateContext.change.addCondition(JdbcEntity.ID, entity.databaseAdapter.idApplicator(updateContext.id));
        JdbcBranchCommit branchCommit = new JdbcBranchCommit(offset, updateContext.change);

        for (Entity entity : value.getList()) {
          Map<String, Entity> map = entity.getMap();

          BranchCommit bc = branchCommit.id(Id.fromEntity(map.get("id")))
              .commit(Id.fromEntity(map.get("commit")));

          if (map.containsKey("parent")) {
            SavedCommit saved = bc.saved();
            saved.parent(Id.fromEntity(map.get("parent"))).done();
          } else {
            UnsavedCommitDelta unsaved = bc.unsaved();
            if (map.containsKey("deltas")) {
              for (Entity delta : map.get("deltas").getList()) {
                Map<String, Entity> deltaMap = delta.getMap();
                unsaved = unsaved.delta(
                    Ints.saturatedCast(deltaMap.get("position").getNumber()),
                    Id.fromEntity(deltaMap.get("old")),
                    Id.fromEntity(deltaMap.get("new"))
                );
              }
            }
            UnsavedCommitMutations mutations = unsaved.mutations();
            if (map.containsKey("keys")) {
              for (Entity delta : map.get("keys").getList()) {
                Map<String, Entity> keysMap = delta.getMap();
                if (keysMap.containsKey("a")) {
                  mutations = mutations.keyMutation(keyFromEntity(keysMap.get("a")).asAddition());
                } else if (keysMap.containsKey("d")) {
                  mutations = mutations.keyMutation(keyFromEntity(keysMap.get("d")).asRemoval());
                }
              }
            }
            mutations.done();
          }
        }
        return;
      default:
        break;
    }

    super.updateListAppend(updateContext, path, value);
  }

  @Override
  void removeValue(UpdateContext updateContext, ExpressionPath path) {
    NameSegment root = path.getRoot();

    String rootName = root.getName();
    switch (rootName) {
      case "commits":
        if (!root.getChild().isPresent()) {
          throw new UnsupportedOperationException("remove of 'commits' without child not supported " + path);
        }
        PathSegment child = root.getChild().get();
        int adjust = updateContext.adjustedIndex(rootName);
        int pos = adjust + child.asPosition().getPosition();
        if (child.getChild().isPresent()) {
          // remove commit attribute
          String attrName = child.getChild().get().asName().getName();

          String col = "c_" + attrName + makeSuffix(pos);
          updateContext.change.setExpression(
              col,
              col + " = NULL");
        } else {
          // remove whole commit, shift all array "elements"

          for (int i = pos; i < MAX_COMMITS + adjust - 1; i++) {
            for (String commitCol : C_COLUMNS) {
              String col = commitCol + makeSuffix(i);
              updateContext.change.setExpression(col,
                  commitCol + makeSuffix(i) + " = " + commitCol + makeSuffix(i + 1 - adjust));
            }
          }
          // set last branch-commit to "null"
          for (int i = MAX_COMMITS - 1 + adjust; i < MAX_COMMITS; i++) {
            for (String commitCol : C_COLUMNS) {
              String col = commitCol + makeSuffix(i);
              updateContext.change.setExpression(col,
                  col + " = NULL");
            }
          }

          updateContext.adjustIndex(rootName);
        }
        return;
      default:
        break;
    }

    super.removeValue(updateContext, path);
  }

  private int nextCommitIndex(Id id) {
    try {
      PreparedStatement selectStmt = resources
          .add(resources.connection.prepareStatement("SELECT "
              + IntStream.range(0, MAX_COMMITS).mapToObj(i -> C_ID + makeSuffix(i))
              .collect(Collectors.joining(", "))
              + " FROM "
              + entity.tableName
              + " WHERE "
              + JdbcEntity.ID + " = ?"));
      entity.databaseAdapter.setId(selectStmt, 1, id);
      ResultSet rs = resources.add(selectStmt.executeQuery());
      int offset = 0;
      if (rs.next()) {
        for (int i = 0; i < MAX_COMMITS; i++) {
          if (entity.databaseAdapter.getId(rs, C_ID + makeSuffix(i)) == null) {
            break;
          }
          offset++;
        }
      }
      return offset;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts an {@link Entity} to a {@link Key}.
   */
  static Key keyFromEntity(Entity a) {
    return Key.of(a.getList().stream().map(Entity::getString).toArray(String[]::new));
  }
}
