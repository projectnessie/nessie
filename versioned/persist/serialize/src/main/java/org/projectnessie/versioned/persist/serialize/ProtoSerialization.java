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
package org.projectnessie.versioned.persist.serialize;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentsId;
import org.projectnessie.versioned.persist.adapter.ContentsIdAndBytes;
import org.projectnessie.versioned.persist.adapter.ContentsIdWithType;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;

public class ProtoSerialization {
  public static AdapterTypes.CommitLogEntry toProto(CommitLogEntry entry) {
    AdapterTypes.CommitLogEntry.Builder proto =
        AdapterTypes.CommitLogEntry.newBuilder()
            .setCreatedTime(entry.getCreatedTime())
            .setHash(entry.getHash().asBytes())
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
            .metadata(proto.getMetadata())
            .keyListDistance(proto.getKeyListDistance());

    proto.getParentsList().forEach(p -> entry.addParents(Hash.of(p)));
    proto.getPutsList().forEach(p -> entry.addPuts(protoToKeyWithBytes(p)));
    proto.getDeletesList().forEach(p -> entry.addDeletes(protoToKey(p)));
    if (!proto.getKeyListList().isEmpty()) {
      ImmutableKeyList.Builder kl = ImmutableKeyList.builder();
      proto.getKeyListList().forEach(kle -> kl.addKeys(protoToKeyWithType(kle)));
      entry.keyList(kl.build());
    }
    proto.getKeyListIdsList().forEach(p -> entry.addKeyListsIds(Hash.of(p)));

    return entry.build();
  }

  public static AdapterTypes.ContentsIdWithBytes toProto(ContentsIdAndBytes x) {
    return AdapterTypes.ContentsIdWithBytes.newBuilder()
        .setContentsId(AdapterTypes.ContentsId.newBuilder().setId(x.getContentsId().getId()))
        .setType(x.getType())
        .setValue(x.getValue())
        .build();
  }

  public static ContentsIdAndBytes protoToContentsIdAndBytes(
      AdapterTypes.ContentsIdWithBytes proto) {
    return ContentsIdAndBytes.of(
        ContentsId.of(proto.getContentsId().getId()), (byte) proto.getType(), proto.getValue());
  }

  public static AdapterTypes.ContentsIdWithType toProto(ContentsIdWithType x) {
    return AdapterTypes.ContentsIdWithType.newBuilder()
        .setContentsId(AdapterTypes.ContentsId.newBuilder().setId(x.getContentsId().getId()))
        .setType(x.getType())
        .build();
  }

  public static ContentsIdWithType protoToContentsIdWithType(
      AdapterTypes.ContentsIdWithType proto) {
    return ContentsIdWithType.of(
        ContentsId.of(proto.getContentsId().getId()), (byte) proto.getType());
  }

  public static AdapterTypes.KeyList toProto(KeyList x) {
    AdapterTypes.KeyList.Builder keyList = AdapterTypes.KeyList.newBuilder();
    for (KeyWithType key : x.getKeys()) {
      keyList.addKeys(toProto(key));
    }
    return keyList.build();
  }

  public static KeyList protoToKeyList(ByteString serialized) {
    try {
      AdapterTypes.KeyList proto = AdapterTypes.KeyList.parseFrom(serialized);
      ImmutableKeyList.Builder keyList = ImmutableKeyList.builder();
      for (AdapterTypes.KeyWithType key : proto.getKeysList()) {
        keyList.addKeys(protoToKeyWithType(key));
      }
      return keyList.build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static KeyList protoToKeyList(byte[] bytes) {
    try {
      AdapterTypes.KeyList proto = AdapterTypes.KeyList.parseFrom(bytes);
      ImmutableKeyList.Builder keyList = ImmutableKeyList.builder();
      for (AdapterTypes.KeyWithType key : proto.getKeysList()) {
        keyList.addKeys(protoToKeyWithType(key));
      }
      return keyList.build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static AdapterTypes.KeyWithBytes toProto(KeyWithBytes x) {
    return AdapterTypes.KeyWithBytes.newBuilder()
        .setKey(keyToProto(x.getKey()))
        .setContentsId(AdapterTypes.ContentsId.newBuilder().setId(x.getContentsId().getId()))
        .setType(x.getType())
        .setValue(x.getValue())
        .build();
  }

  public static KeyWithBytes protoToKeyWithBytes(AdapterTypes.KeyWithBytes proto) {
    return KeyWithBytes.of(
        Key.of(proto.getKey().getElementList().toArray(new String[0])),
        ContentsId.of(proto.getContentsId().getId()),
        (byte) proto.getType(),
        proto.getValue());
  }

  public static AdapterTypes.KeyWithType toProto(KeyWithType x) {
    return AdapterTypes.KeyWithType.newBuilder()
        .setKey(keyToProto(x.getKey()))
        .setContentsId(AdapterTypes.ContentsId.newBuilder().setId(x.getContentsId().getId()))
        .setType(x.getType())
        .build();
  }

  public static KeyWithType protoToKeyWithType(AdapterTypes.KeyWithType proto) {
    return KeyWithType.of(
        protoToKey(proto.getKey()),
        ContentsId.of(proto.getContentsId().getId()),
        (byte) proto.getType());
  }

  public static AdapterTypes.Key keyToProto(Key key) {
    return AdapterTypes.Key.newBuilder().addAllElement(key.getElements()).build();
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
