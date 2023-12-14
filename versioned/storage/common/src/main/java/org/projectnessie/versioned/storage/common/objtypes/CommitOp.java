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
package org.projectnessie.versioned.storage.common.objtypes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.versioned.storage.common.persist.ObjId.deserializeObjId;
import static org.projectnessie.versioned.storage.common.persist.ObjId.skipObjId;
import static org.projectnessie.versioned.storage.common.util.Ser.putVarInt;
import static org.projectnessie.versioned.storage.common.util.Ser.readVarInt;
import static org.projectnessie.versioned.storage.common.util.Ser.skipVarInt;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.indexes.ElementSerializer;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/** Represents an operation for a {@link StoreKey key} in a {@link StoreIndex store index}. */
@Value.Immutable
public interface CommitOp {

  @Value.Parameter(order = 1)
  Action action();

  @Value.Parameter(order = 2)
  int payload();

  @Value.Parameter(order = 3)
  @Nullable
  ObjId value();

  // Note: the content-ID from legacy, imported Nessie repositories could theoretically been
  // any string value. If it's a UUID, use it, otherwise ignore it down the road.
  @Value.Parameter(order = 4)
  @Nullable
  UUID contentId();

  static CommitOp commitOp(@Nonnull Action action, int payload, @Nullable ObjId value) {
    return commitOp(action, payload, value, null);
  }

  static CommitOp commitOp(
      @Nonnull Action action, int payload, @Nullable ObjId value, @Nullable UUID contentId) {
    checkArgument(payload >= 0 && payload <= 127);
    checkArgument(value == null || value.size() > 0);
    return ImmutableCommitOp.of(action, payload, value, contentId);
  }

  ElementSerializer<CommitOp> COMMIT_OP_SERIALIZER = new CommitEntrySerializer();

  /**
   * Commit-op actions declare whether a particular key/value was added or removed in the current
   * commit, whether a key/value was added or removed in a previous commit or whether it is just
   * present.
   *
   * <p>See the description of {@link CommitObj} for details.
   *
   * <p>Note: these actions do <em>not</em> map 1:1 to the put/delete operations in Nessie commits.
   */
  enum Action {
    /** Placeholder for actions in {@link CommitObj#referenceIndex() full indexes}. */
    NONE('N', true, false),

    /** "Add key" action added in the current commit. */
    ADD('A', true, true),
    /** "Remove key" action added in the current commit. */
    REMOVE('R', false, true),

    /**
     * "Add key" action added in a previous commit, used to mark {@link #ADD} actions from previous
     * commits in the {@link CommitObj#incrementalIndex() incremental index}.
     */
    INCREMENTAL_ADD('a', true, false),
    /**
     * "Delete key" action added in a previous commit, used to mark {@link #REMOVE} actions from
     * previous commits in the {@link CommitObj#incrementalIndex() incremental index}.
     */
    INCREMENTAL_REMOVE('r', false, false);

    public static Action fromValue(char value) {
      switch (value) {
        case 'N':
          return NONE;
        case 'A':
          return ADD;
        case 'R':
          return REMOVE;
        case 'a':
          return INCREMENTAL_ADD;
        case 'r':
          return INCREMENTAL_REMOVE;
        default:
          throw new IllegalArgumentException("Illegal value '" + value + "' for Operation");
      }
    }

    private final char value;
    private final boolean exists;
    private final boolean currentCommit;

    Action(char value, boolean exists, boolean currentCommit) {
      this.value = value;
      this.exists = exists;
      this.currentCommit = currentCommit;
    }

    public char value() {
      return value;
    }

    public boolean exists() {
      return exists;
    }

    public boolean currentCommit() {
      return currentCommit;
    }
  }

  final class CommitEntrySerializer implements ElementSerializer<CommitOp> {

    @Override
    public int serializedSize(CommitOp value) {
      ObjId v = value.value();
      if (v == null) {
        v = ObjId.zeroLengthObjId();
      }
      return
      // Action
      1
          // Payload
          + 1
          // ObjId
          + v.serializedSize()
          // content ID (UUID)
          + 16;
    }

    @Override
    public ByteBuffer serialize(CommitOp value, ByteBuffer target) {
      target.put((byte) value.action().value());
      putVarInt(target, value.payload());
      ObjId v = value.value();
      if (v == null) {
        v = ObjId.zeroLengthObjId();
      }
      v.serializeTo(target);
      UUID contentId = value.contentId();
      if (contentId != null) {
        target
            .putLong(contentId.getMostSignificantBits())
            .putLong(contentId.getLeastSignificantBits());
      } else {
        target.putLong(0L).putLong(0L);
      }
      return target;
    }

    @Override
    public CommitOp deserialize(ByteBuffer buffer) {
      Action operation = Action.fromValue((char) buffer.get());
      int payload = readVarInt(buffer);
      ObjId id = deserializeObjId(buffer);
      if (id.size() == 0) {
        id = null;
      }
      long msb = buffer.getLong();
      long lsb = buffer.getLong();
      UUID contentId = (msb != 0L || lsb != 0L) ? new UUID(msb, lsb) : null;
      return commitOp(operation, payload, id, contentId);
    }

    @Override
    public void skip(ByteBuffer buffer) {
      // Action
      buffer.get();
      // Payload
      skipVarInt(buffer);
      // ObjId
      skipObjId(buffer);
      // content ID (UUID MSB + LSB)
      buffer.position(buffer.position() + 16);
    }
  }

  /**
   * Converts the given {@code String} into a {@code UUID}, if possible, otherwise return {@code
   * null}.
   */
  static UUID contentIdMaybe(String contentId) {
    try {
      return UUID.fromString(contentId);
    } catch (Exception e) {
      return null;
    }
  }
}
