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

import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.CommitMetadata;

import com.google.protobuf.ByteString;

final class CommitMetadataObj extends WrappedValueObj<CommitMetadata> {

  CommitMetadataObj(Id id, long dt, ByteString value) {
    super(id, dt, value);
  }

  @Override
  BaseObj<CommitMetadata> copy() {
    return new CommitMetadataObj(getId(), getDt(), getValue());
  }

  static class CommitMetadataProducer extends BaseWrappedValueObjProducer<CommitMetadata> implements CommitMetadata {
    @Override
    BaseObj<CommitMetadata> build() {
      return new CommitMetadataObj(getId(), getDt(), getValue());
    }
  }
}
