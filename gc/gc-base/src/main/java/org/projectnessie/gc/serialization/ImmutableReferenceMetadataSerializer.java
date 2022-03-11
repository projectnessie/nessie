/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableReferenceMetadata;

public class ImmutableReferenceMetadataSerializer extends Serializer<ImmutableReferenceMetadata> {
  public ImmutableReferenceMetadataSerializer() {
    this.setImmutable(true);
  }

  @Override
  public void write(Kryo kryo, Output output, ImmutableReferenceMetadata referenceMetadata) {
    kryo.writeObject(output, referenceMetadata.getNumCommitsAhead());
    kryo.writeObject(output, referenceMetadata.getNumTotalCommits());
    kryo.writeObject(output, referenceMetadata.getNumCommitsBehind());
    kryo.writeObject(output, referenceMetadata.getCommitMetaOfHEAD());
    output.writeString(referenceMetadata.getCommonAncestorHash());
  }

  @Override
  public ImmutableReferenceMetadata read(
      Kryo kryo, Input input, Class<ImmutableReferenceMetadata> classType) {
    return ImmutableReferenceMetadata.builder()
        .numCommitsAhead(kryo.readObject(input, Integer.class))
        .numTotalCommits(kryo.readObject(input, Long.class))
        .numCommitsBehind(kryo.readObject(input, Integer.class))
        .commitMetaOfHEAD(kryo.readObject(input, ImmutableCommitMeta.class))
        .commonAncestorHash(input.readString())
        .build();
  }
}
