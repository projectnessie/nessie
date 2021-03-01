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
package org.projectnessie.versioned.tiered.gc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.versioned.impl.Ids;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.L1;
import org.projectnessie.versioned.tiered.Mutation;

/**
 * Class used for L1 records within Spark.
 */
public class L1Frame implements Serializable {

  private static final long serialVersionUID = 2021838527611235712L;

  private IdFrame id;
  private long dt;
  private List<IdFrame> children;
  private List<L1Parent> parents;

  public IdFrame getId() {
    return id;
  }

  public void setId(IdFrame id) {
    this.id = id;
  }

  public long getDt() {
    return dt;
  }

  public void setDt(long dt) {
    this.dt = dt;
  }

  public List<L1Parent> getParents() {
    return parents;
  }

  public void setParents(List<L1Parent> parents) {
    this.parents = parents;
  }

  public List<IdFrame> getChildren() {
    return children;
  }

  public void setChildren(List<IdFrame> children) {
    this.children = children;
  }

  public L1Frame() {
  }

  /**
   * Construct L1Frame.
   */
  public L1Frame(Id id, long dt, Stream<Id> unfilteredParents, List<Id> children) {
    this.id = IdFrame.of(id);
    this.dt = dt;

    this.parents = toParents(unfilteredParents);
    this.children = children.stream().map(IdFrame::of).collect(Collectors.toList());
  }

  private static List<L1Parent> toParents(Stream<Id> unfilteredParents) {
    List<Id> parents = unfilteredParents.filter(Id::isNonEmpty).collect(Collectors.toList());
    List<L1Parent> values = new ArrayList<L1Parent>(parents.size());
    for (int i = 0; i < parents.size(); i++) {
      values.add(L1Parent.of(parents.get(i), i == parents.size() - 1));
    }
    return values;
  }

  public static class L1Parent implements Serializable {
    private static final long serialVersionUID = 2264533513613785665L;

    private IdFrame id;
    private boolean recurse;

    /**
     * Construct L1Parent.
     */
    public static L1Parent of(Id id, boolean last) {
      L1Parent child = new L1Parent();
      child.id = IdFrame.of(id);
      child.recurse = last;
      return child;
    }

    public IdFrame getId() {
      return id;
    }

    public void setId(IdFrame id) {
      this.id = id;
    }

    public boolean isRecurse() {
      return recurse;
    }

    public void setRecurse(boolean recurse) {
      this.recurse = recurse;
    }

    @Override
    public String toString() {
      return "L1Child [id=" + id + ", recurse=" + recurse + "]";
    }

  }

  private L1 capture() {
    return new L1() {

      @Override
      public L1 id(Id id) {
        L1Frame.this.id = new IdFrame(id.toBytes());
        return this;
      }

      @Override
      public L1 dt(long dt) {
        L1Frame.this.dt = dt;
        return this;
      }

      @Override
      public L1 commitMetadataId(Id id) {
        return this;
      }

      @Override
      public L1 ancestors(Stream<Id> ids) {
        L1Frame.this.parents = toParents(ids);
        return this;
      }

      @Override
      public L1 children(Stream<Id> ids) {
        L1Frame.this.children = ids.map(IdFrame::of).collect(Collectors.toList());
        return this;
      }

      @Override
      public L1 keyMutations(Stream<Mutation> keyMutations) {
        return this;
      }

      @Override
      public L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
        return this;
      }

      @Override
      public L1 completeKeyList(Stream<Id> fragmentIds) {
        return this;
      }
    };
  }

  @Override
  public String toString() {
    return "L1Frame [id=" + id + ", dt=" + dt + ", children=" + children + ", parents=" + parents + "]";
  }

  public static Function<Store.Acceptor<L1>, L1Frame> BUILDER = (a) -> {
    L1Frame f = new L1Frame();
    a.applyValue(f.capture());
    return f;
  };

  private static final byte[] EMPTY_L1_BYTES = Ids.getEmptyL1().toBytes();

  /**
   * A filter that excludes the "empty" L1.
   */
  private static final Predicate<L1Frame> EXCLUDE_EMPTY_L1 = l -> !Arrays.equals(EMPTY_L1_BYTES, l.getId().getId());

  public static Dataset<L1Frame> asDataset(Supplier<Store> store, SparkSession spark) {
    return ValueRetriever.dataset(store, ValueType.L1, L1Frame.class, Optional.of(EXCLUDE_EMPTY_L1), spark, BUILDER);
  }
}
