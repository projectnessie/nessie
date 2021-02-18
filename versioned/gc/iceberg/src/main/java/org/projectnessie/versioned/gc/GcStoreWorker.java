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
package org.projectnessie.versioned.gc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.server.providers.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;

import com.google.protobuf.ByteString;

/**
 * Store worker specific to GC ops. Requires the base StoreWorker used by the server.
 */
public class GcStoreWorker implements StoreWorker<Contents, CommitMeta> {
  private final Serializer<Contents> valueWorker = new ContentsValueSerializer();
  private final Serializer<CommitMeta> metadataSerializer = new CommitMetaValueSerializer();

  public GcStoreWorker() {
  }

  @Override
  public Serializer<Contents> getValueSerializer() {
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

  /**
   * ValueWorker which wraps the default worker for reading a Contents.
   *
   * <p>This augments the default worker by making it Externalizable.
   */
  private static class ContentsValueSerializer implements Serializer<Contents>, Externalizable {
    private final Serializer<Contents> innerWorker = new TableCommitMetaStoreWorker().getValueSerializer();

    public ContentsValueSerializer() {

    }

    @Override
    public ByteString toBytes(Contents value) {
      return innerWorker.toBytes(value);
    }

    @Override
    public Contents fromBytes(ByteString bytes) {
      return innerWorker.fromBytes(bytes);
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

    }
  }
}
