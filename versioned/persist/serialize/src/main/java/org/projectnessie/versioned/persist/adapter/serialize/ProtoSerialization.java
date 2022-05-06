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
package org.projectnessie.versioned.persist.adapter.serialize;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.TreeMap;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.ImmutableRefLog;
import org.projectnessie.versioned.persist.adapter.ImmutableRepoDescription;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RepoProps;

public class ProtoSerialization {

  public static RepoProps toProto(RepoDescription repoDescription) {
    RepoProps.Builder proto =
        AdapterTypes.RepoProps.newBuilder().setRepoVersion(repoDescription.getRepoVersion());
    // Must be sorted
    new TreeMap<>(repoDescription.getProperties())
        .forEach(
            (k, v) -> proto.addProperties(AdapterTypes.Entry.newBuilder().setKey(k).setValue(v)));
    return proto.build();
  }

  public static RepoDescription protoToRepoDescription(ByteString bytes) {
    if (bytes == null) {
      return RepoDescription.DEFAULT;
    }
    try {
      AdapterTypes.RepoProps proto = AdapterTypes.RepoProps.parseFrom(bytes);
      return protoToRepoDescription(proto);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static RepoDescription protoToRepoDescription(byte[] bytes) {
    if (bytes == null) {
      return RepoDescription.DEFAULT;
    }
    try {
      AdapterTypes.RepoProps proto = AdapterTypes.RepoProps.parseFrom(bytes);
      return protoToRepoDescription(proto);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static RepoDescription protoToRepoDescription(AdapterTypes.RepoProps proto) {
    ImmutableRepoDescription.Builder repoDesc =
        RepoDescription.builder().repoVersion(proto.getRepoVersion());
    proto.getPropertiesList().forEach(e -> repoDesc.putProperties(e.getKey(), e.getValue()));
    return repoDesc.build();
  }

  public static AdapterTypes.CommitLogEntry toProto(CommitLogEntry entry) {
    AdapterTypes.CommitLogEntry.Builder proto =
        AdapterTypes.CommitLogEntry.newBuilder()
            .setCreatedTime(entry.getCreatedTime())
            .setHash(entry.getHash().asBytes())
            .setCommitSeq(entry.getCommitSeq())
            .setMetadata(entry.getMetadata())
            .setKeyListDistance(entry.getKeyListDistance());

    entry.getParents().forEach(p -> proto.addParents(p.asBytes()));
    entry.getPuts().forEach(p -> proto.addPuts(toProto(p)));
    entry.getDeletes().forEach(p -> proto.addDeletes(keyToProto(p)));

    if (entry.getKeyList() != null) {
      entry.getKeyList().getKeys().forEach(k -> proto.addKeyList(toProto(k)));
    }
    entry.getKeyListsIds().forEach(k -> proto.addKeyListIds(k.asBytes()));

    return proto.build();
  }

  public static CommitLogEntry protoToCommitLogEntry(ByteString serialized) {
    try {
      if (serialized == null) {
        return null;
      }

      AdapterTypes.CommitLogEntry proto = AdapterTypes.CommitLogEntry.parseFrom(serialized);
      return protoToCommitLogEntry(proto);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static CommitLogEntry protoToCommitLogEntry(byte[] bytes) {
    try {
      if (bytes == null) {
        return null;
      }

      AdapterTypes.CommitLogEntry proto = AdapterTypes.CommitLogEntry.parseFrom(bytes);
      return protoToCommitLogEntry(proto);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static CommitLogEntry protoToCommitLogEntry(AdapterTypes.CommitLogEntry proto) {
    ImmutableCommitLogEntry.Builder entry =
        ImmutableCommitLogEntry.builder()
            .createdTime(proto.getCreatedTime())
            .hash(Hash.of(proto.getHash()))
            .commitSeq(proto.getCommitSeq())
            .metadata(proto.getMetadata())
            .keyListDistance(proto.getKeyListDistance());

    proto.getParentsList().forEach(p -> entry.addParents(Hash.of(p)));
    proto.getPutsList().forEach(p -> entry.addPuts(protoToKeyWithBytes(p)));
    proto.getDeletesList().forEach(p -> entry.addDeletes(protoToKey(p)));
    if (!proto.getKeyListList().isEmpty()) {
      ImmutableKeyList.Builder kl = ImmutableKeyList.builder();
      proto.getKeyListList().forEach(kle -> kl.addKeys(protoToKeyListEntry(kle)));
      entry.keyList(kl.build());
    }
    proto.getKeyListIdsList().forEach(p -> entry.addKeyListsIds(Hash.of(p)));

    return entry.build();
  }

  public static AdapterTypes.ContentIdWithBytes toProto(ContentIdAndBytes x) {
    return AdapterTypes.ContentIdWithBytes.newBuilder()
        .setContentId(AdapterTypes.ContentId.newBuilder().setId(x.getContentId().getId()))
        .setValue(x.getValue())
        .build();
  }

  public static ContentIdAndBytes protoToContentIdAndBytes(AdapterTypes.ContentIdWithBytes proto) {
    return ContentIdAndBytes.of(ContentId.of(proto.getContentId().getId()), proto.getValue());
  }

  public static AdapterTypes.ContentIdWithType toProto(ContentId x) {
    return AdapterTypes.ContentIdWithType.newBuilder()
        .setContentId(AdapterTypes.ContentId.newBuilder().setId(x.getId()))
        .setTypeUnused(0)
        .build();
  }

  public static ContentId protoToContentId(AdapterTypes.ContentIdWithType proto) {
    return ContentId.of(proto.getContentId().getId());
  }

  public static AdapterTypes.KeyList toProto(KeyList x) {
    AdapterTypes.KeyList.Builder keyList = AdapterTypes.KeyList.newBuilder();
    for (KeyListEntry key : x.getKeys()) {
      keyList.addKeys(toProto(key));
    }
    return keyList.build();
  }

  public static KeyList protoToKeyList(ByteString serialized) {
    try {
      AdapterTypes.KeyList proto = AdapterTypes.KeyList.parseFrom(serialized);
      return protoToKeyList(proto);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static KeyList protoToKeyList(byte[] bytes) {
    try {
      AdapterTypes.KeyList proto = AdapterTypes.KeyList.parseFrom(bytes);
      return protoToKeyList(proto);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static ImmutableKeyList protoToKeyList(AdapterTypes.KeyList proto) {
    ImmutableKeyList.Builder keyList = ImmutableKeyList.builder();
    for (AdapterTypes.KeyListEntry key : proto.getKeysList()) {
      keyList.addKeys(protoToKeyListEntry(key));
    }
    return keyList.build();
  }

  public static AdapterTypes.KeyWithBytes toProto(KeyWithBytes x) {
    return AdapterTypes.KeyWithBytes.newBuilder()
        .setKey(keyToProto(x.getKey()))
        .setContentId(AdapterTypes.ContentId.newBuilder().setId(x.getContentId().getId()))
        .setType(x.getType())
        .setValue(x.getValue())
        .build();
  }

  public static KeyWithBytes protoToKeyWithBytes(AdapterTypes.KeyWithBytes proto) {
    return KeyWithBytes.of(
        Key.of(proto.getKey().getElementList().toArray(new String[0])),
        ContentId.of(proto.getContentId().getId()),
        (byte) proto.getType(),
        proto.getValue());
  }

  public static AdapterTypes.KeyListEntry toProto(KeyListEntry x) {
    AdapterTypes.KeyListEntry.Builder builder =
        AdapterTypes.KeyListEntry.newBuilder()
            .setKey(keyToProto(x.getKey()))
            .setContentId(AdapterTypes.ContentId.newBuilder().setId(x.getContentId().getId()))
            .setType(x.getType());
    if (x.getCommitId() != null) {
      builder.setCommitId(x.getCommitId().asBytes());
    }
    return builder.build();
  }

  public static KeyListEntry protoToKeyListEntry(AdapterTypes.KeyListEntry proto) {
    return KeyListEntry.of(
        protoToKey(proto.getKey()),
        ContentId.of(proto.getContentId().getId()),
        (byte) proto.getType(),
        proto.hasCommitId() ? Hash.of(proto.getCommitId()) : null);
  }

  public static AdapterTypes.Key keyToProto(Key key) {
    return AdapterTypes.Key.newBuilder().addAllElement(key.getElements()).build();
  }

  public static RefLog protoToRefLog(ByteString serialized) {
    try {
      if (serialized == null) {
        return null;
      }
      AdapterTypes.RefLogEntry proto = AdapterTypes.RefLogEntry.parseFrom(serialized);
      return protoToRefLog(proto);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static RefLog protoToRefLog(byte[] bytes) {
    try {
      if (bytes == null) {
        return null;
      }
      AdapterTypes.RefLogEntry proto = AdapterTypes.RefLogEntry.parseFrom(bytes);
      return protoToRefLog(proto);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static RefLog protoToRefLog(AdapterTypes.RefLogEntry proto) {
    ImmutableRefLog.Builder entry =
        ImmutableRefLog.builder()
            .refLogId(Hash.of(proto.getRefLogId()))
            .refName(proto.getRefName().toStringUtf8())
            .refType(proto.getRefType().name())
            .commitHash(Hash.of(proto.getCommitHash()))
            .operationTime(proto.getOperationTime())
            .operation(proto.getOperation().name());
    proto.getParentsList().forEach(parentId -> entry.addParents(Hash.of(parentId)));
    proto.getSourceHashesList().forEach(hash -> entry.addSourceHashes(Hash.of(hash)));
    return entry.build();
  }

  public static Key protoToKey(AdapterTypes.Key key) {
    return Key.of(key.getElementList().toArray(new String[0]));
  }

  /** Functional interface for the various {@code protoToABC()} methods above. */
  @FunctionalInterface
  public interface Parser<T> {
    T parse(byte[] data) throws InvalidProtocolBufferException;
  }
}
