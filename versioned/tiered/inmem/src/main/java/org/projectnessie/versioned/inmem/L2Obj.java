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
package org.projectnessie.versioned.inmem;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.L2;

final class L2Obj extends BaseObj<L2> {
  private static final String CHILDREN = "children";
  private final List<Id> children;

  L2Obj(Id id, long dt, List<Id> children) {
    super(id, dt);
    this.children = children;
  }

  @Override
  L2 consume(L2 consumer) {
    return super.consume(consumer).children(children.stream());
  }

  static class L2Producer extends BaseObjProducer<L2> implements L2 {
    private List<Id> children;

    @Override
    public L2 children(Stream<Id> ids) {
      this.children = ids.collect(Collectors.toList());
      return this;
    }

    @Override
    BaseObj<L2> build() {
      return new L2Obj(getId(), getDt(), children);
    }
  }

  @Override
  BaseObj<L2> copy() {
    return new L2Obj(getId(), getDt(), new ArrayList<>(children));
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case CHILDREN:
        evaluate(function, children);
        break;
      default:
        // Invalid Condition Function.
        throw new ConditionFailedException(invalidOperatorSegmentMessage(function));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    L2Obj that = (L2Obj) o;

    return children.equals(that.children);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + children.hashCode();
    return result;
  }
}
