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

import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.BaseWrappedValue;

import com.google.protobuf.ByteString;

abstract class WrappedValueObj<C extends BaseWrappedValue<C>> extends BaseObj<C> {
  static final String VALUE = "value";
  private final ByteString value;

  WrappedValueObj(Id id, long dt, ByteString value) {
    super(id, dt);
    this.value = value;
  }

  ByteString getValue() {
    return value;
  }

  @Override
  C consume(C consumer) {
    return super.consume(consumer).value(value);
  }

  abstract static class BaseWrappedValueObjProducer<C extends BaseWrappedValue<C>>
      extends BaseObjProducer<C> implements BaseWrappedValue<C> {

    private ByteString value;

    ByteString getValue() {
      return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public C value(ByteString value) {
      this.value = value;
      return (C) this;
    }
  }

  @Override
  public void evaluate(Function function) throws ConditionFailedException {
    final String segment = function.getRootPathAsNameSegment().getName();
    switch (segment) {
      case ID:
        evaluatesId(function);
        break;
      case VALUE:
        if (!function.isRootNameSegmentChildlessAndEquals()
            || !value.equals(function.getValue().getBinary())) {
          throw new ConditionFailedException(conditionNotMatchedMessage(function));
        }
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

    WrappedValueObj<?> that = (WrappedValueObj<?>) o;

    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }
}
