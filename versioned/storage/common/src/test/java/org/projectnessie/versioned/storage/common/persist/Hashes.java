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
package org.projectnessie.versioned.storage.common.persist;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX_SEGMENTS;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.REF;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.STRING;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.TAG;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.UNIQUE;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.VALUE;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.List;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;

/**
 * Contains the code that generated {@link ObjId}s before {@link ObjIdHasher} was introduced, used
 * to validate compatibility with the old code. This class and (parts of) the corresponding test may
 * eventually go away.
 */
@SuppressWarnings("UnstableApiUsage")
final class Hashes {
  private Hashes() {}

  public static Hasher newHasher() {
    return Hashing.sha256().newHasher();
  }

  public static ObjId hashAsObjId(Hasher hasher) {
    return ObjId.objIdFromByteArray(hasher.hash().asBytes());
  }

  static ObjId indexSegmentsHash(List<IndexStripe> stripes) {
    Hasher hasher = newHasher();
    hasher.putString(INDEX_SEGMENTS.name(), UTF_8);
    for (IndexStripe indexStripe : stripes) {
      hasher.putString(indexStripe.firstKey().rawString(), UTF_8);
      hasher.putString(indexStripe.lastKey().rawString(), UTF_8);
      hasher.putBytes(indexStripe.segment().asByteBuffer());
    }
    return hashAsObjId(hasher);
  }

  static ObjId indexHash(ByteString idx) {
    return hashAsObjId(indexHash(newHasher().putString(INDEX.name(), UTF_8), idx));
  }

  static Hasher indexHash(Hasher hasher, ByteString idx) {
    return hasher.putBytes(idx.asReadOnlyByteBuffer());
  }

  static ObjId contentValueHash(String contentId, int payload, ByteString data) {
    Hasher hasher =
        newHasher()
            .putString(VALUE.name(), UTF_8)
            .putString(contentId, UTF_8)
            .putInt(payload)
            .putBytes(data.asReadOnlyByteBuffer());
    return hashAsObjId(hasher);
  }

  static ObjId refHash(String name, ObjId initialPointer, long createdAtMicros) {
    Hasher hasher =
        newHasher()
            .putString(REF.name(), UTF_8)
            .putString(name, UTF_8)
            .putBytes(initialPointer.asByteArray())
            .putLong(createdAtMicros);
    return hashAsObjId(hasher);
  }

  static ObjId tagHash(String message, CommitHeaders headers, ByteString signature) {
    Hasher hasher = newHasher().putString(TAG.name(), UTF_8);
    if (message != null) {
      hasher.putString(message, UTF_8);
    }
    if (headers != null) {
      hashCommitHeaders(hasher, headers);
    }
    if (signature != null) {
      hasher.putBytes(signature.asReadOnlyByteBuffer());
    }
    return hashAsObjId(hasher);
  }

  public static void hashCommitHeaders(Hasher hasher, CommitHeaders headers) {
    for (String header : headers.keySet()) {
      hasher.putString(header, UTF_8);
      List<String> values = headers.getAll(header);
      for (String value : values) {
        hasher.putString(value, UTF_8);
      }
    }
  }

  static ObjId stringDataHash(
      String contentType,
      Compression compression,
      String filename,
      List<ObjId> predecessors,
      ByteString text) {
    Hasher hasher =
        newHasher()
            .putString(STRING.name(), UTF_8)
            .putString(contentType, UTF_8)
            .putChar(compression.value());
    if (filename != null) {
      hasher.putString(filename, UTF_8);
    }
    predecessors.forEach(id -> hasher.putBytes(id.asByteArray()));
    hasher.putBytes(text.asReadOnlyByteBuffer());
    return hashAsObjId(hasher);
  }

  static ObjId uniqueIdHash(String space, ByteString value) {
    Hasher hasher =
        newHasher()
            .putString(UNIQUE.name(), UTF_8)
            .putString(space, UTF_8)
            .putBytes(value.asReadOnlyByteBuffer());
    return hashAsObjId(hasher);
  }
}
