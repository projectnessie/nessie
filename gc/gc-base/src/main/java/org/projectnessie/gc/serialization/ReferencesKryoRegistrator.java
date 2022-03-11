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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.serializer.KryoRegistrator;
import org.projectnessie.gc.base.ContentBloomFilter;
import org.projectnessie.gc.base.ContentValues;
import org.projectnessie.gc.base.DistributedIdentifyContents;
import org.projectnessie.gc.base.IdentifiedResult;
import org.projectnessie.gc.base.IdentifyContentsPerExecutor;
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

  @Override
  public void registerClasses(Kryo kryo) {
    LOGGER.info(
        "Registering classes for kryo: "
            + kryo
            + Arrays.toString(Thread.currentThread().getStackTrace()));
    kryo.register(ImmutableBranch.class, new ImmutableBranchSerializer());
    kryo.register(ImmutableDetached.class, new ImmutableDetachedSerializer());
    kryo.register(ImmutableReferenceMetadata.class, new ImmutableReferenceMetadataSerializer());
    kryo.register(ImmutableCommitMeta.class, new ImmutableCommitMetaSerializer());
    kryo.register(ImmutableTag.class, new ImmutableTagSerializer());
    kryo.register(ContentBloomFilter.class, new ContentBloomFilterSerializer());
    kryo.register(ImmutableGCParams.class, new ImmutableGCParamsSerializer());
    kryo.register(HashMap.class);
    kryo.register(ImmutableIcebergTable.class);
    kryo.register(HashSet.class);
    kryo.register(ConcurrentHashMap.class);
    kryo.register(IdentifiedResult.class);
    kryo.register(ContentValues.class);
    kryo.register(IdentifyContentsPerExecutor.class);
    kryo.register(DistributedIdentifyContents.class);
    //    kryo.register(Class.class);
    //    kryo.register(java.lang.invoke.SerializedLambda.class);
    //    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
  }

  //
  //  private static MapSerializer referenceToInstantMapSerializer() {
  //    MapSerializer serializer = new MapSerializer();
  //    serializer.setKeysCanBeNull(false);
  //    serializer.setKeyClass(ImmutableBranch.class, new ImmutableBranchSerializer());
  //    serializer.setValuesCanBeNull(false);
  //    serializer.setValueClass(Instant.class, new InstantSerializer());
  //    return serializer;
  //  }
}
