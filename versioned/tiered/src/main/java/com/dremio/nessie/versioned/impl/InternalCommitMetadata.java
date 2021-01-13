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

import com.dremio.nessie.tiered.builder.CommitMetadataConsumer;
import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
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

  /**
   * Create a new {@link Builder} instance that implements both
   * {@link CommitMetadataConsumer} and a matching {@link Producer} that
   * builds an {@link InternalCommitMetadata} object.
   *
   * @return new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Implements both
   * {@link CommitMetadataConsumer} and a matching {@link Producer} that
   * builds an {@link InternalCommitMetadata} object.
   */
  // Needs to be a public class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  public static final class Builder
      extends WrappedValueBean.Builder<InternalCommitMetadata, CommitMetadataConsumer>
      implements CommitMetadataConsumer, Producer<InternalCommitMetadata, CommitMetadataConsumer> {

    Builder() {
      super(InternalCommitMetadata::new);
    }
  }
}
