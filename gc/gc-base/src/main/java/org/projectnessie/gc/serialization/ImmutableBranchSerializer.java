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
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableReferenceMetadata;

public class ImmutableBranchSerializer extends Serializer<ImmutableBranch> {

  public ImmutableBranchSerializer() {
    this.setImmutable(true);
  }

  @Override
  public void write(Kryo kryo, Output output, ImmutableBranch immutableBranch) {
    output.writeString(immutableBranch.getName());
    output.writeString(immutableBranch.getHash());
    kryo.writeObjectOrNull(output, immutableBranch.getMetadata(), ImmutableReferenceMetadata.class);
  }

  @Override
  public ImmutableBranch read(Kryo kryo, Input input, Class classType) {
    return ImmutableBranch.builder()
        .name(input.readString())
        .hash(input.readString())
        .metadata(kryo.readObjectOrNull(input, ImmutableReferenceMetadata.class))
        .build();
  }
}
