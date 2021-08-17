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
import com.google.protobuf.UnsafeByteOperations;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

/** Utility methods for {@link DatabaseAdapter} implementations. */
public final class DatabaseAdapterUtil {
  private DatabaseAdapterUtil() {}

  @SuppressWarnings("UnstableApiUsage")
  public static Hasher newHasher() {
    return Hashing.sha256().newHasher();
  }

  @SuppressWarnings("UnstableApiUsage")
  public static Hash randomHash() {
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    Hasher hasher = newHasher();
    for (int i = 0; i < 20; i++) {
      hasher.putInt(rand.nextInt());
    }
    return Hash.of(UnsafeByteOperations.unsafeWrap(hasher.hash().asBytes()));
  }

  @SuppressWarnings("UnstableApiUsage")
  public static void hashKey(Hasher hasher, Key k) {
    k.getElements().forEach(e -> hasher.putString(e, StandardCharsets.UTF_8));
  }

  public static ReferenceConflictException hashCollisionDetected() {
    return new ReferenceConflictException("Hash collision detected");
  }

  /** Builds a {@link ReferenceNotFoundException} exception with a human-readable message. */
  public static ReferenceNotFoundException hashNotFound(NamedRef ref, Hash hash) {
    return new ReferenceNotFoundException(
        String.format(
            "Could not find commit '%s' in reference '%s'.", hash.asString(), ref.getName()));
  }

  public static ReferenceNotFoundException referenceNotFound(NamedRef ref) {
    return new ReferenceNotFoundException(
        String.format("Named reference '%s' not found", ref.getName()));
  }

  public static ReferenceNotFoundException referenceNotFound(Hash hash) {
    return new ReferenceNotFoundException(String.format("Commit '%s' not found", hash.asString()));
  }

  public static ReferenceAlreadyExistsException referenceAlreadyExists(NamedRef ref) {
    return new ReferenceAlreadyExistsException(
        String.format("Named reference '%s' already exists.", ref.getName()));
  }

  public static String mergeConflictMessage(
      String err, Hash from, BranchName toBranch, Optional<Hash> expectedHead) {
    return String.format(
        "%s during merge of '%s' into '%s%s' requiring a common ancestor",
        err,
        from.asString(),
        toBranch.getName(),
        expectedHead.map(h -> "@" + h.asString()).orElse(""));
  }

  public static String transplantConflictMessage(
      String err,
      BranchName targetBranch,
      Optional<Hash> expectedHead,
      List<Hash> sequenceToTransplant) {
    return String.format(
        "%s during transplant of %d commits into '%s%s'",
        err,
        sequenceToTransplant.size(),
        targetBranch.getName(),
        expectedHead.map(h -> "@" + h.asString()).orElse(""));
  }

  public static String commitConflictMessage(
      String err, BranchName commitTo, Optional<Hash> expectedHead) {
    return String.format(
        "%s during commit against '%s%s'",
        err, commitTo.getName(), expectedHead.map(h -> "@" + h.asString()).orElse(""));
  }

  public static String createConflictMessage(String err, NamedRef ref, Hash target) {
    return String.format(
        "%s during create of reference '%s' at '%s'", err, ref.getName(), target.asString());
  }

  public static String deleteConflictMessage(
      String err, NamedRef reference, Optional<Hash> expectedHead) {
    return String.format(
        "%s during delete of reference '%s%s'",
        err, reference.getName(), expectedHead.map(h -> "@" + h.asString()).orElse(""));
  }

  public static String assignConflictMessage(
      String err, NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo) {
    return String.format(
        "%s during reassign of reference %s%s to '%s'",
        err,
        assignee.getName(),
        expectedHead.map(h -> "@" + h.asString()).orElse(""),
        assignTo.asString());
  }

  /**
   * Verifies that {@code expectedHead}, if present, is equal to {@code referenceCurrentHead}.
   * Throws a {@link ReferenceConflictException} if not.
   */
  public static void verifyExpectedHash(
      Hash referenceCurrentHead, NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceConflictException {
    if (expectedHead.isPresent() && !referenceCurrentHead.equals(expectedHead.get())) {
      throw new ReferenceConflictException(
          String.format(
              "Named-reference '%s' is not at expected hash '%s', but at '%s'.",
              reference.getName(), expectedHead.get().asString(), referenceCurrentHead.asString()));
    }
  }

  /**
   * Lets the given {@link Stream} stop when {@link Predicate predicate} returns {@code true}, does
   * not return the "last" element for which the predicate returned {@code true}.
   *
   * @param stream stream to watch/wrap
   * @param stopPredicate if this predicate returns {@code true}, the stop-condition is triggered
   */
  public static <T> Stream<T> takeUntilExcludeLast(Stream<T> stream, Predicate<T> stopPredicate) {
    return takeUntil(stream, stopPredicate, false);
  }

  /**
   * Lets the given {@link Stream} stop when {@link Predicate predicate} returns {@code true}, does
   * return the "last" element for which the predicate returned {@code true}.
   *
   * @param stream stream to watch/wrap
   * @param stopPredicate if this predicate returns {@code true}, the stop-condition is triggered
   */
  public static <T> Stream<T> takeUntilIncludeLast(Stream<T> stream, Predicate<T> stopPredicate) {
    return takeUntil(stream, stopPredicate, true);
  }

  /**
   * See {@link #takeUntilIncludeLast(Stream, Predicate)} and {@link #takeUntilExcludeLast(Stream,
   * Predicate)}.
   */
  private static <T> Stream<T> takeUntil(
      Stream<T> stream, Predicate<T> stopPredicate, boolean includeLast) {
    Spliterator<T> src = stream.spliterator();

    AbstractSpliterator<T> split =
        new AbstractSpliterator<T>(src.estimateSize(), 0) {
          boolean done = false;

          @Override
          public boolean tryAdvance(Consumer<? super T> consumer) {
            if (done) {
              return false;
            }
            return src.tryAdvance(
                elem -> {
                  boolean t = stopPredicate.test(elem);
                  if (t && !includeLast) {
                    done = true;
                  }
                  if (!done) {
                    consumer.accept(elem);
                  }
                  if (t && includeLast) {
                    done = true;
                  }
                });
          }
        };

    // Convert (back) to a Stream and ensure that '.close()' is propagated.
    return StreamSupport.stream(split, false).onClose(stream::close);
  }
}
