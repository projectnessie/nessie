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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.KeyDelta;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.Store.Acceptor;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;
import org.projectnessie.versioned.tiered.L2;
import org.projectnessie.versioned.tiered.L3;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Utility object used to encapsulate an id object that references additional ids.
 *
 * <p>Example potential uses include the following relationships: L1 to L2, L2 to L3, L3 to value.
 */
public class IdCarrier implements Serializable {

  private static final long serialVersionUID = -1421784708484600778L;

  private IdFrame id;
  private List<IdFrame> children;
  private Map<IdFrame, List<String>> childKeys;

  public List<IdFrame> getChildren() {
    return children;
  }

  public IdFrame getId() {
    return id;
  }

  public void setId(IdFrame id) {
    this.id = id;
  }

  public void setChildren(List<IdFrame> children) {
    this.children = children;
  }

  public Map<IdFrame, List<String>> getChildKeys() {
    return childKeys;
  }

  public void setChildKeys(Map<IdFrame, List<String>> childKeys) {
    this.childKeys = childKeys;
  }

  public static <T extends BaseValue<T>> Dataset<IdCarrier> asDataset(
      ValueType<T> valueType,
      Supplier<Store> store,
      Function<Acceptor<T>, IdCarrier> converter,
      Optional<Predicate<IdCarrier>> predicate,
      SparkSession spark) {
    return new ValueRetriever<T, IdCarrier>(store, valueType, converter, IdCarrier.class).get(spark, predicate);
  }

  public static final Function<Acceptor<L2>, IdCarrier> L2_CONVERTER = a -> {
    IdCarrier c = new IdCarrier();
    a.applyValue(new L2() {

      @Override
      public L2 id(Id id) {
        c.id = IdFrame.of(id);
        return this;
      }

      @Override
      public L2 dt(long dt) {
        // IdCarrier doesn't currently need dt so dropping information.
        return this;
      }

      @Override
      public L2 children(Stream<Id> ids) {
        c.children = ids.map(IdFrame::of).collect(Collectors.toList());
        return this;
      }
    });
    return c;
  };

  public static final Function<Acceptor<L3>, IdCarrier> L3_CONVERTER = a -> {
    IdCarrier c = new IdCarrier();
    a.applyValue(new L3() {

      @Override
      public L3 id(Id id) {
        c.id = IdFrame.of(id);
        return this;
      }

      @Override
      public L3 dt(long dt) {
        return this;
      }

      @Override
      public L3 keyDelta(Stream<KeyDelta> keyDelta) {
        List<IdFrame> children = Lists.newArrayList();
        Map<IdFrame, List<String>> childKeys = Maps.newHashMap();
        keyDelta.forEach(kd -> {
          IdFrame id = IdFrame.of(kd.getId());
          children.add(id);
          childKeys.put(id, kd.getKey().getElements());
        });
        c.childKeys = childKeys;
        c.children = children;
        return this;
      }
    });
    return c;
  };
}
