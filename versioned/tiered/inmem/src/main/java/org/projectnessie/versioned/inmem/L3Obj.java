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
import org.projectnessie.versioned.store.KeyDelta;
import org.projectnessie.versioned.tiered.L3;

final class L3Obj extends BaseObj<L3> {
  private final List<KeyDelta> keyDelta;

  L3Obj(Id id, long dt, List<KeyDelta> keyDelta) {
    super(id, dt);
    this.keyDelta = keyDelta;
  }

  @Override
  L3 consume(L3 consumer) {
    return super.consume(consumer).keyDelta(keyDelta.stream());
  }

  static class L3Producer extends BaseObjProducer<L3> implements L3 {
    private List<KeyDelta> keyDelta;

    @Override
    public L3 keyDelta(Stream<KeyDelta> keyDelta) {
      this.keyDelta = keyDelta.collect(Collectors.toList());
      return this;
    }

    @Override
    BaseObj<L3> build() {
      return new L3Obj(getId(), getDt(), keyDelta);
    }
  }

  @Override
  BaseObj<L3> copy() {
    return new L3Obj(getId(), getDt(),
        keyDelta != null ? new ArrayList<>(keyDelta) : null);
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();
    if (segment.equals(ID)) {
      evaluatesId(function);
    } else {
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

    L3Obj that = (L3Obj) o;

    return keyDelta.equals(that.keyDelta);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + keyDelta.hashCode();
    return result;
  }
}
