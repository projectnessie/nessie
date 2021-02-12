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

import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.CommitMetadata;

import com.google.protobuf.ByteString;

class InternalCommitMetadata extends WrappedValueBean<CommitMetadata> {

  private InternalCommitMetadata(Id id, ByteString value, Long dt) {
    super(id, value, dt);
  }

  static InternalCommitMetadata of(ByteString value) {
    return new InternalCommitMetadata(null, value, DT.now());
  }

  @Override
  protected long getSeed() {
    return 2279557414590649190L;// an arbitrary but consistent seed to ensure no hash conflicts.
  }

  /**
   * Implements {@link CommitMetadata} to build an {@link InternalCommitMetadata} object.
   */
  // Needs to be a package private class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  static final class Builder extends WrappedValueBean.Builder<InternalCommitMetadata, CommitMetadata>
      implements CommitMetadata {
    Builder() {
      super(InternalCommitMetadata::new);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  EntityType<CommitMetadata, InternalCommitMetadata, InternalCommitMetadata.Builder> getEntityType() {
    return EntityType.COMMIT_METADATA;
  }
}
