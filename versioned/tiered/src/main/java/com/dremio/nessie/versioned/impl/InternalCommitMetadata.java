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

import com.dremio.nessie.tiered.builder.CommitMetadataConsumer;
import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;

public class InternalCommitMetadata extends WrappedValueBean<CommitMetadataConsumer> {

  private InternalCommitMetadata(Id id, ByteString value) {
    super(id, value);
  }

  public static InternalCommitMetadata of(ByteString value) {
    return new InternalCommitMetadata(null, value);
  }

  @Override
  protected long getSeed() {
    return 2279557414590649190L;// an arbitrary but consistent seed to ensure no hash conflicts.
  }

  public static final SimpleSchema<InternalCommitMetadata> SCHEMA =
      new WrappedValueBean.WrappedValueSchema<>(InternalCommitMetadata.class, InternalCommitMetadata::new);

  @Override
  public CommitMetadataConsumer applyToConsumer(CommitMetadataConsumer consumer) {
    return consumer.id(getId())
        .value(getBytes());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements CommitMetadataConsumer, Producer<InternalCommitMetadata, CommitMetadataConsumer> {

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
      return valueType == ValueType.COMMIT_METADATA;
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
    public InternalCommitMetadata build() {
      checkSet(id, "id");
      checkSet(value, "value");

      return new InternalCommitMetadata(id, value);
    }
  }
}
