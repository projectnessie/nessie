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
package org.projectnessie.versioned.storage.common.exceptions;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.Obj;

/**
 * Exception thrown when an object could not be stored because there is an existing version of it,
 * and that version does not match the current object's version.
 */
public class ObjMismatchException extends Exception {

  @Value.Immutable
  @Value.Style(underrideToString = "asText")
  public interface ObjMismatch {

    @Value.Parameter(order = 1)
    Obj getExistingObj();

    @Value.Parameter(order = 2)
    Obj getExpectedObj();

    default String asText() {
      return "existing: " + getExistingObj() + ", expected: " + getExpectedObj();
    }

    @Nonnull
    @jakarta.annotation.Nonnull
    static ObjMismatch of(
        @Nonnull @jakarta.annotation.Nonnull Obj existingObj,
        @Nonnull @jakarta.annotation.Nonnull Obj expectedObj) {
      return ImmutableObjMismatch.of(existingObj, expectedObj);
    }
  }

  public static void checkAndThrow(
      @Nonnull @jakarta.annotation.Nonnull Obj[] existing,
      @Nonnull @jakarta.annotation.Nonnull Obj[] expected)
      throws ObjMismatchException {
    checkArgument(existing.length == expected.length, "Arrays must be of same length.");
    List<ObjMismatch> mismatches = null;
    for (int i = 0; i < existing.length; i++) {
      if (expected[i] != null && existing[i] != null && !expected[i].equals(existing[i])) {
        if (mismatches == null) {
          mismatches = new ArrayList<>();
        }
        mismatches.add(ObjMismatch.of(existing[i], expected[i]));
      }
    }
    if (mismatches != null && !mismatches.isEmpty()) {
      throw new ObjMismatchException(mismatches);
    }
  }

  private final List<ObjMismatch> mismatches;

  public ObjMismatchException(Obj existingObj, Obj expectedObj) {
    this(ImmutableObjMismatch.of(existingObj, expectedObj));
  }

  public ObjMismatchException(ObjMismatch mismatch) {
    super("Object does not match: " + mismatch);
    this.mismatches = List.of(mismatch);
  }

  public ObjMismatchException(List<ObjMismatch> mismatches) {
    super(
        mismatches.size() == 1
            ? "Object does not match: " + mismatches.get(0).asText()
            : "Objects do not match: "
                + mismatches.stream().map(ObjMismatch::asText).collect(joining("; ")));
    this.mismatches = List.copyOf(mismatches);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public List<ObjMismatch> getMismatches() {
    return mismatches;
  }
}
