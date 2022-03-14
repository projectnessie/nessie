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
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.serializer.KryoRegistrator;
import org.projectnessie.gc.base.ContentBloomFilter;
import org.projectnessie.gc.base.ContentValues;
import org.projectnessie.gc.base.IdentifiedResult;
import org.projectnessie.gc.base.ImmutableGCParams;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDetached;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.ImmutableTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReferencesKryoRegistrator implements KryoRegistrator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReferencesKryoRegistrator.class);

  /**
   * Registering all needed classes. We need to register all used classes, because we are setting
   * `spark.kryo.registrationRequired` to `true`. Setting it to false may impact the performance
   * significantly, because as per doc:
   *
   * <p>Writing class names can cause significant performance overhead, so enabling this option can
   * enforce strictly that a user has not omitted classes from registration.
   *
   * @see <a href="https://spark.apache.org/docs/latest/configuration.html">the Spark
   *     documentation</a>
   * @param kryo
   */
  @Override
  public void registerClasses(Kryo kryo) {
    LOGGER.info("Registering classes for kryo: " + kryo);
    kryo.register(ImmutableBranch.class, new ImmutableBranchSerializer());
    kryo.register(ImmutableDetached.class, new ImmutableDetachedSerializer());
    kryo.register(ImmutableReferenceMetadata.class, new ImmutableReferenceMetadataSerializer());
    kryo.register(ImmutableCommitMeta.class, new ImmutableCommitMetaSerializer());
    kryo.register(ImmutableTag.class, new ImmutableTagSerializer());
    kryo.register(ContentBloomFilter.class, new ContentBloomFilterSerializer());
    kryo.register(ImmutableGCParams.class, new ImmutableGCParamsSerializer());
    kryo.register(ImmutableIcebergTable.class);
    kryo.register(IdentifiedResult.class);
    kryo.register(ContentValues.class);

    // needed for date-time
    kryo.register(Duration.class);
    kryo.register(Instant.class);

    // needed for all types of collections. It will use serializers from
    // `com.esotericsoftware.kryo.serializers.DefaultSerializers`
    kryo.register(HashMap.class);
    kryo.register(HashSet.class);
    kryo.register(ConcurrentHashMap.class);
    kryo.register(LinkedHashMap.class);
    kryo.register(Collections.EMPTY_LIST.getClass());
    kryo.register(Collections.EMPTY_MAP.getClass());
    kryo.register(Collections.EMPTY_SET.getClass());
    kryo.register(Collections.singletonList("").getClass());
    kryo.register(Collections.singleton("").getClass());
    kryo.register(Collections.singletonMap("", "").getClass());

    // needed for java.util.Collections$UnmodifiableMap
    UnmodifiableCollectionsSerializer.registerSerializers(kryo);
    ImmutableMapSerializer.registerSerializers(kryo);
  }
}
