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
package com.dremio.nessie.versioned.gc;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.util.SerializableConfiguration;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.server.providers.TableCommitMetaStoreWorker;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.ValueWorker;
import com.google.protobuf.ByteString;

/**
 * Store worker specifc to GC ops. Requires the base StoreWorker used by the server.
 */
public class GcStoreWorker implements StoreWorker<Contents, CommitMeta> {
  private final ValueWorker<Contents> valueWorker;
  private final Serializer<CommitMeta> metadataSerializer = new CommitMetaValueSerializer();

  public GcStoreWorker(Configuration hadoopConfig) {
    valueWorker = new ContentsValueWorker(new SerializableConfiguration(hadoopConfig));
  }

  @Override
  public ValueWorker<Contents> getValueWorker() {
    return valueWorker;
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metadataSerializer;
  }

  /**
   * Delegate commit meta worker to underlying store worker.
   *
   * <p>The only reason this is needed is the worker has to be serializable.
   */
  private static class CommitMetaValueSerializer implements Serializer<CommitMeta>, Serializable {
    private final Serializer<CommitMeta> innerWorker = new TableCommitMetaStoreWorker().getMetadataSerializer();

    public CommitMetaValueSerializer() {
    }

    @Override
    public ByteString toBytes(CommitMeta value) {
      return innerWorker.toBytes(value);
    }

    @Override
    public CommitMeta fromBytes(ByteString bytes) {
      return innerWorker.fromBytes(bytes);
    }
  }
}
