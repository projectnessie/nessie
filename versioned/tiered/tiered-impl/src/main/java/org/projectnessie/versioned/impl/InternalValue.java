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
package org.projectnessie.versioned.impl;

import com.google.protobuf.ByteString;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.Value;

/** Holds a VersionStore binary value for interaction with the Store. */
class InternalValue extends WrappedValueBean<Value> {

  private InternalValue(Id id, ByteString value, Long dt) {
    super(id, value, dt);
  }

  static InternalValue of(ByteString value) {
    return new InternalValue(null, value, DT.now());
  }

  @Override
  protected long getSeed() {
    return 2829568831168137780L; // an arbitrary but consistent seed to ensure no hash conflicts.
  }

  /** Implements {@link Value} to builds an {@link InternalValue} object. */
  // Needs to be a package private class, otherwise class-initialization of ValueType fails with
  // j.l.IllegalAccessError
  static final class Builder extends WrappedValueBean.Builder<InternalValue, Value>
      implements Value {
    Builder() {
      super(InternalValue::new);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  EntityType<Value, InternalValue, InternalValue.Builder> getEntityType() {
    return EntityType.VALUE;
  }
}
