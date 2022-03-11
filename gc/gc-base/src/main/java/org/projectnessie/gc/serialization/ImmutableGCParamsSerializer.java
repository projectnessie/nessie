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
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import org.projectnessie.gc.base.ImmutableGCParams;

public class ImmutableGCParamsSerializer extends Serializer<ImmutableGCParams> {
  public ImmutableGCParamsSerializer() {
    this.setImmutable(true);
  }

  @Override
  public void write(Kryo kryo, Output output, ImmutableGCParams immutableGCParams) {
    kryo.writeClassAndObject(output, immutableGCParams.getNessieClientConfigs());
    kryo.writeClassAndObject(output, immutableGCParams.getCutOffTimestampPerRef());
    kryo.writeObject(output, immutableGCParams.getDefaultCutOffTimestamp());
    kryo.writeObjectOrNull(
        output, immutableGCParams.getDeadReferenceCutOffTimeStamp(), Instant.class);
    kryo.writeObjectOrNull(output, immutableGCParams.getSparkPartitionsCount(), Integer.class);
    kryo.writeObject(output, immutableGCParams.getCommitProtectionDuration());
    kryo.writeObjectOrNull(output, immutableGCParams.getBloomFilterExpectedEntries(), Long.class);
    output.writeDouble(immutableGCParams.getBloomFilterFpp());
  }

  @Override
  @SuppressWarnings("unchecked")
  public ImmutableGCParams read(Kryo kryo, Input input, Class<ImmutableGCParams> classType) {
    return ImmutableGCParams.builder()
        .nessieClientConfigs(kryo.readObject(input, HashMap.class))
        .cutOffTimestampPerRef(kryo.readObject(input, HashMap.class))
        .defaultCutOffTimestamp(kryo.readObject(input, Instant.class))
        .deadReferenceCutOffTimeStamp(kryo.readObjectOrNull(input, Instant.class))
        .sparkPartitionsCount(kryo.readObjectOrNull(input, Integer.class))
        .commitProtectionDuration(kryo.readObject(input, Duration.class))
        .bloomFilterExpectedEntries(kryo.readObject(input, Long.class))
        .bloomFilterFpp(input.readDouble())
        .build();
  }
}
