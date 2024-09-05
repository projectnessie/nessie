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

import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.STRING;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import jakarta.annotation.Nullable;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjIdHasher;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
public interface StringObj extends Obj {

  @Override
  default ObjType type() {
    return StandardObjType.STRING;
  }

  @Override
  @Value.Parameter(order = 1)
  @Nullable
  ObjId id();

  @Override
  @Value.Parameter(order = 1)
  @Value.Auxiliary
  long referenced();

  @Value.Parameter(order = 2)
  String contentType();

  @Value.Parameter(order = 3)
  Compression compression();

  @Nullable
  @Value.Parameter(order = 4)
  String filename();

  /**
   * The predecessors of this version represent a base content plus 0 or more diffs plus {@link
   * #text()} representing the last diff.
   *
   * <p>If empty, the {@link #text()} represents the full content.
   */
  @Value.Parameter(order = 5)
  List<ObjId> predecessors();

  /**
   * Represents the full content, if {@link #predecessors()} is empty, or the last diff based on the
   * {@link #predecessors()}.
   *
   * <p>Contains the UTF-8 representation of a {@link String}, if <nobr>{@link #compression()}{@code
   * ==}{@link Compression#NONE}</nobr> or the compressed representation of it.
   */
  @Value.Parameter(order = 6)
  ByteString text();

  static StringObj stringData(
      ObjId id,
      long referenced,
      String contentType,
      Compression compression,
      @Nullable String filename,
      List<ObjId> predecessors,
      ByteString text) {
    return ImmutableStringObj.of(
        id, referenced, contentType, compression, filename, predecessors, text);
  }

  static StringObj stringData(
      String contentType,
      Compression compression,
      @Nullable String filename,
      List<ObjId> predecessors,
      ByteString text) {
    ObjIdHasher hasher =
        objIdHasher(STRING).hash(contentType).hash(compression.value()).hash(filename);
    predecessors.forEach(id -> hasher.hash(id.asByteArray()));
    hasher.hash(text.asReadOnlyByteBuffer());

    return stringData(
        hasher.generate(), 0L, contentType, compression, filename, predecessors, text);
  }
}
