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

import java.util.TreeMap;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.nessie.relocated.protobuf.InvalidProtocolBufferException;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry.KeyListVariant;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.ImmutableRepoDescription;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;

public final class ProtoSerialization {

  private ProtoSerialization() {}

  public static AdapterTypes.RepoProps toProto(RepoDescription repoDescription) {
    AdapterTypes.RepoProps.Builder proto =
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

    proto.setKeyListVariant(AdapterTypes.KeyListVariant.valueOf(entry.getKeyListVariant().name()));

    entry.getParents().forEach(p -> proto.addParents(p.asBytes()));
    entry.getPuts().forEach(p -> proto.addPuts(toProto(p)));
    entry.getDeletes().forEach(p -> proto.addDeletes(keyToProto(p)));

    if (entry.getKeyList() != null) {
      entry.getKeyList().getKeys().forEach(k -> proto.addKeyList(toProto(k)));
    }
    entry.getKeyListsIds().forEach(k -> proto.addKeyListIds(k.asBytes()));
    if (entry.getKeyListEntityOffsets() != null) {
      proto.addAllKeyListEntityOffsets(entry.getKeyListEntityOffsets());
    }
    if (entry.getKeyListLoadFactor() != null) {
      proto.setKeyListLoadFactor(entry.getKeyListLoadFactor());
      proto.setKeyListBucketCount(entry.getKeyListBucketCount());
    }
    entry.getAdditionalParents().forEach(p -> proto.addAdditionalParents(p.asBytes()));

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

    KeyListVariant keyListVariant =
        proto.hasKeyListVariant()
            ? KeyListVariant.valueOf(proto.getKeyListVariant().name())
            : KeyListVariant.EMBEDDED_AND_EXTERNAL_MRU;
    entry.keyListVariant(keyListVariant);

    proto.getParentsList().forEach(p -> entry.addParents(Hash.of(p)));
    proto.getPutsList().forEach(p -> entry.addPuts(protoToKeyWithBytes(p)));
    proto.getDeletesList().forEach(p -> entry.addDeletes(protoToKey(p)));
    if (!proto.getKeyListList().isEmpty()) {
      ImmutableKeyList.Builder kl = ImmutableKeyList.builder();
      proto.getKeyListList().forEach(kle -> kl.addKeys(protoToKeyListEntry(kle)));
      entry.keyList(kl.build());
    } else if (keyListVariant != KeyListVariant.EMBEDDED_AND_EXTERNAL_MRU) {
      // keyListVariant != EMBEDDED_AND_EXTERNAL_MRU means that the commit aggregates all visible
      // keys. Adding an empty key list here triggers that detection. Future Nessie versions can
      // then change key-list aggregation to omit the embedded key list and only persist
      // key-list-entities, which allows bigger hash-buckets.
      entry.keyList(KeyList.EMPTY);
    }
    proto.getKeyListIdsList().forEach(p -> entry.addKeyListsIds(Hash.of(p)));
    entry.addAllKeyListEntityOffsets(proto.getKeyListEntityOffsetsList());
    proto.getAdditionalParentsList().forEach(p -> entry.addAdditionalParents(Hash.of(p)));
    if (proto.hasKeyListLoadFactor()) {
      entry.keyListLoadFactor(proto.getKeyListLoadFactor());
      entry.keyListBucketCount(proto.getKeyListBucketCount());
    }

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
        .setPayload(x.getPayload())
        .setValue(x.getValue())
        .build();
  }

  public static KeyWithBytes protoToKeyWithBytes(AdapterTypes.KeyWithBytes proto) {
    return KeyWithBytes.of(
        ContentKey.of(proto.getKey().getElementList().toArray(new String[0])),
        ContentId.of(proto.getContentId().getId()),
        (byte) proto.getPayload(),
        proto.getValue());
  }

  public static AdapterTypes.KeyListEntry toProto(KeyListEntry x) {
    if (x == null) {
      return AdapterTypes.KeyListEntry.getDefaultInstance();
    }
    AdapterTypes.KeyListEntry.Builder builder =
        AdapterTypes.KeyListEntry.newBuilder()
            .setKey(keyToProto(x.getKey()))
            .setContentId(AdapterTypes.ContentId.newBuilder().setId(x.getContentId().getId()))
            .setPayload(x.getPayload());
    if (x.getCommitId() != null) {
      builder.setCommitId(x.getCommitId().asBytes());
    }
    return builder.build();
  }

  public static KeyListEntry protoToKeyListEntry(AdapterTypes.KeyListEntry proto) {
    if (!proto.hasKey()) {
      return null;
    }
    return KeyListEntry.of(
        protoToKey(proto.getKey()),
        ContentId.of(proto.getContentId().getId()),
        (byte) proto.getPayload(),
        proto.hasCommitId() ? Hash.of(proto.getCommitId()) : null);
  }

  public static AdapterTypes.Key keyToProto(ContentKey key) {
    return AdapterTypes.Key.newBuilder().addAllElement(key.getElements()).build();
  }

  public static ContentKey protoToKey(AdapterTypes.Key key) {
    return ContentKey.of(key.getElementList().toArray(new String[0]));
  }

  /** Functional interface for the various {@code protoToABC()} methods above. */
  @FunctionalInterface
  public interface Parser<T> {
    T parse(byte[] data) throws InvalidProtocolBufferException;
  }
}
