/*
 * Copyright (C) 2023 Dremio
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

import org.projectnessie.versioned.storage.common.logic.InternalRef;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public enum StandardObjType implements ObjType {
  /**
   * Identifies a named reference and contains the initial referencee.
   *
   * <p>Managed in the well-known internal reference {@link InternalRef#REF_REFS}.
   *
   * <p>{@link Obj} is a {@link RefObj}.
   */
  REF("r"),

  /** {@link Obj} is a {@link CommitObj}. */
  COMMIT("c"),

  /** {@link Obj} is a {@link TagObj}. */
  TAG("t"),

  /** {@link Obj} is a {@link ContentValueObj}. */
  VALUE("v"),

  /** {@link Obj} is a {@link StringObj}. */
  STRING("s"),

  /** {@link Obj} is a {@link IndexSegmentsObj}. */
  INDEX_SEGMENTS("I"),

  /** {@link Obj} is a {@link IndexObj}. */
  INDEX("i");

  private final String shortName;

  StandardObjType(String shortName) {
    this.shortName = shortName;
  }

  @Override
  public String shortName() {
    return shortName;
  }
}
