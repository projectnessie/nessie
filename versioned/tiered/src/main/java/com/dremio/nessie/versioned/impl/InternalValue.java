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

import static com.dremio.nessie.versioned.impl.ValidationHelper.checkCalled;
import static com.dremio.nessie.versioned.impl.ValidationHelper.checkSet;

import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.tiered.builder.ValueConsumer;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;

/**
 * Holds a VersionStore binary value for interaction with the Store.
 */
public class InternalValue extends WrappedValueBean<ValueConsumer> {

  private InternalValue(Id id, ByteString value) {
    super(id, value);
  }

  public static InternalValue of(ByteString value) {
    return new InternalValue(null, value);
  }

  @Override
  protected long getSeed() {
    return 2829568831168137780L; // an arbitrary but consistent seed to ensure no hash conflicts.
  }

  public static final SimpleSchema<InternalValue> SCHEMA =
      new WrappedValueBean.WrappedValueSchema<>(InternalValue.class, InternalValue::new);

  @Override
  public ValueConsumer applyToConsumer(ValueConsumer consumer) {
    return consumer.id(getId())
        .value(getBytes());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements ValueConsumer, Producer<InternalValue, ValueConsumer> {

    protected Id id;
    protected ByteString value;

    @Override
    public Builder id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return this;
    }

    @Override
    public boolean canHandleType(ValueType valueType) {
      return valueType == ValueType.VALUE;
    }

    @Override
    public Builder value(ByteString value) {
      checkCalled(this.value, "value");
      this.value = value;
      return this;
    }

    /**
     * TODO javadoc.
     */
    public InternalValue build() {
      checkSet(id, "id");
      checkSet(value, "value");

      return new InternalValue(id, value);
    }
  }
}
