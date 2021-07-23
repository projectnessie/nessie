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
package org.projectnessie.versioned.persist.adapter.spi;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

/**
 * Utility methods for {@link DatabaseAdapter} implementations.
 *
 * <p>This class is just there to keep {@link AbstractDatabaseAdapter} cleaner and shorter.
 */
public abstract class DatabaseAdapterUtil implements DatabaseAdapter {

  @SuppressWarnings("UnstableApiUsage")
  protected static Hasher newHasher() {
    return Hashing.sha256().newHasher();
  }

  @SuppressWarnings("UnstableApiUsage")
  protected static void hashKey(Hasher hasher, Key k) {
    k.getElements().forEach(e -> hasher.putString(e, StandardCharsets.UTF_8));
  }

  protected static ReferenceConflictException hashCollisionDetected() {
    return new ReferenceConflictException("Hash collision detected");
  }

  /** Builds a {@link ReferenceNotFoundException} exception with a human-readable message. */
  protected static ReferenceNotFoundException hashNotFound(NamedRef ref, Hash hash) {
    return new ReferenceNotFoundException(
        String.format(
            "Could not find commit '%s' in reference '%s'.", hash.asString(), ref.getName()));
  }

  protected static ReferenceNotFoundException refNotFound(NamedRef ref) {
    return new ReferenceNotFoundException(
        String.format("Named reference '%s' not found", ref.getName()));
  }

  protected static ReferenceAlreadyExistsException referenceAlreadyExists(NamedRef ref) {
    return new ReferenceAlreadyExistsException(
        String.format("Named reference '%s' already exists.", ref));
  }

  /** Returns the microseconds since epoch. */
  protected static long currentTimeInMicros() {
    // We only have System.currentTimeMillis() as the current wall-clock-value, but want
    // microsecond "precision" here, which is implemented by taking the microsecond-part from
    // System.nanoTime().
    long microsFraction = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
    microsFraction %= 1000L;
    return TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) + microsFraction;
  }

  protected static String mergeConflictMessage(
      String err,
      NamedRef from,
      Optional<Hash> fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash) {
    return String.format(
        "%s during merge of %s%s into %s%s requiring a common ancestor",
        err,
        from.getName(),
        fromHash.map(h -> "@" + h.asString()).orElse(""),
        toBranch.getName(),
        expectedHash.map(h -> "@" + h.asString()).orElse(""));
  }

  protected static String transplantConflictMessage(
      String err,
      BranchName targetBranch,
      Optional<Hash> expectedHash,
      NamedRef source,
      List<Hash> sequenceToTransplant) {
    return String.format(
        "%s during transplant of %d commits from '%s' into '%s%s'",
        err,
        sequenceToTransplant.size(),
        source.getName(),
        targetBranch.getName(),
        expectedHash.map(h -> "@" + h.asString()).orElse(""));
  }

  protected static String commitConflictMessage(
      String err, BranchName branch, Optional<Hash> expectedHead) {
    return String.format(
        "%s during commit against '%s%s'",
        err, branch.getName(), expectedHead.map(h -> "@" + h.asString()).orElse(""));
  }

  protected static String createConflictMessage(
      String err, NamedRef ref, NamedRef target, Optional<Hash> targetHash) {
    return String.format(
        "%s during create of reference '%s' from '%s%s'",
        err, ref.getName(), target, targetHash.map(h -> "@" + h.asString()).orElse(""));
  }

  protected static String deleteConflictMessage(String err, NamedRef ref, Optional<Hash> hash) {
    return String.format(
        "%s during delete of reference '%s%s'",
        err, ref.getName(), hash.map(h -> "@" + h.asString()).orElse(""));
  }

  protected static String assignConflictMessage(
      String err,
      NamedRef ref,
      Optional<Hash> expectedHash,
      NamedRef assignTo,
      Optional<Hash> assignToHash) {
    return String.format(
        "%s during reassign of reference %s%s to '%s%s'",
        err,
        ref.getName(),
        expectedHash.map(h -> "@" + h.asString()).orElse(""),
        assignTo.getName(),
        assignToHash.map(h -> "@" + h.asString()).orElse(""));
  }
}
