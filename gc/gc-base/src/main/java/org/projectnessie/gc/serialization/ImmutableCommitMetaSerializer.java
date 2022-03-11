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
import java.time.Instant;
import java.util.Map;
import org.projectnessie.model.ImmutableCommitMeta;

public class ImmutableCommitMetaSerializer extends Serializer<ImmutableCommitMeta> {

  public ImmutableCommitMetaSerializer() {
    this.setImmutable(true);
  }

  @Override
  public void write(Kryo kryo, Output output, ImmutableCommitMeta immutableCommitMeta) {
    output.writeString(immutableCommitMeta.getCommitter());
    output.writeString(immutableCommitMeta.getAuthor());
    output.writeString(immutableCommitMeta.getHash());
    output.writeString(immutableCommitMeta.getMessage());
    output.writeString(immutableCommitMeta.getSignedOffBy());
    kryo.writeClassAndObject(output, immutableCommitMeta.getProperties());
    kryo.writeObjectOrNull(output, immutableCommitMeta.getCommitTime(), Instant.class);
    kryo.writeObjectOrNull(output, immutableCommitMeta.getAuthorTime(), Instant.class);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ImmutableCommitMeta read(Kryo kryo, Input input, Class<ImmutableCommitMeta> classType) {

    return ImmutableCommitMeta.builder()
        .committer(input.readString())
        .author(input.readString())
        .hash(input.readString())
        .message(input.readString())
        .signedOffBy(input.readString())
        .properties((Map<String, String>) kryo.readClassAndObject(input))
        .commitTime(kryo.readObjectOrNull(input, Instant.class))
        .authorTime(kryo.readObjectOrNull(input, Instant.class))
        .build();
  }
}
