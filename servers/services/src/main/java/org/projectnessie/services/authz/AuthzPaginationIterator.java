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
package org.projectnessie.services.authz;

import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyMap;
import static org.projectnessie.services.authz.BatchAccessChecker.throwForFailedChecks;

import com.google.common.collect.AbstractIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.projectnessie.versioned.paging.PaginationIterator;

public abstract class AuthzPaginationIterator<E> extends AbstractIterator<E>
    implements PaginationIterator<E> {

  private final PaginationIterator<E> source;

  private final Supplier<BatchAccessChecker> checkerSupplier;
  private final int checkBatchSize;
  private final Set<Check> initialChecks;
  private final Set<Check> succeededChecks;
  private final Set<Check> failedChecks;
  private Iterator<E> currentResult = emptyIterator();
  private E currentEntry;

  public AuthzPaginationIterator(
      PaginationIterator<E> source,
      Supplier<BatchAccessChecker> checkerSupplier,
      int checkBatchSize) {
    this.source = source;
    this.checkerSupplier = checkerSupplier;
    this.checkBatchSize = checkBatchSize;
    initialChecks = new HashSet<>();
    succeededChecks = new HashSet<>();
    failedChecks = new HashSet<>();
  }

  public AuthzPaginationIterator<E> initialCheck(Check initialCheck) {
    initialChecks.add(initialCheck);
    return this;
  }

  protected abstract Set<Check> checksForEntry(E entry);

  @Override
  protected final E computeNext() {
    while (true) {
      // Return entries from current result batch
      if (currentResult.hasNext()) {
        E entry = currentResult.next();
        currentEntry = entry;
        return entry;
      }

      // collect batch from source
      List<E> batchEntries = new ArrayList<>(checkBatchSize);
      List<Set<Check>> checksForEntries = new ArrayList<>(checkBatchSize);
      Set<Check> batchChecks = newHashSetWithExpectedSize(checkBatchSize);
      batchChecks.addAll(initialChecks);
      while (source.hasNext()) {
        E entry = source.next();
        Set<Check> checksForEntry = checksForEntry(entry);
        if (checksForEntry.stream().anyMatch(failedChecks::contains)) {
          // At least one of the checks for the current entry failed in a previous
          // batch-access-check,
          // so skip this entry
          continue;
        }

        batchEntries.add(entry);
        checksForEntries.add(checksForEntry);

        checksForEntry.stream().filter(c -> !succeededChecks.contains(c)).forEach(batchChecks::add);

        if (batchChecks.size() >= checkBatchSize) {
          break;
        }
      }

      // Perform batch access check
      Map<Check, String> failed = emptyMap();
      if (!batchChecks.isEmpty()) {
        BatchAccessChecker checker = checkerSupplier.get();
        batchChecks.forEach(checker::can);
        failed = checker.check();
      }

      // Validate the "initial checks" first
      if (!initialChecks.isEmpty()) {
        Map<Check, String> failedInitial = new HashMap<>();
        for (Check initialCheck : initialChecks) {
          String failure = failed.get(initialCheck);
          if (failure != null) {
            failedInitial.put(initialCheck, failure);
          }
        }
        initialChecks.clear();
        if (!failedInitial.isEmpty()) {
          throwForFailedChecks(failedInitial);
        }
      }

      int batchEntryCount = batchEntries.size();
      if (batchEntryCount == 0) {
        // no more entries to check
        return endOfData();
      }

      // Construct the iterator containing the checked entries
      List<E> current = new ArrayList<>(batchEntryCount);
      for (int i = 0; i < batchEntryCount; i++) {
        E entry = batchEntries.get(i);
        Set<Check> checksForEntry = checksForEntries.get(i);
        boolean anyFailed = false;
        for (Check check : checksForEntry) {
          if (failed.containsKey(check)) {
            anyFailed = true;
            failedChecks.add(check);
          } else {
            succeededChecks.add(check);
          }
        }
        if (!anyFailed) {
          current.add(entry);
        }
      }
      currentResult = current.iterator();
    }
  }

  @Override
  public final String tokenForCurrent() {
    return source.tokenForEntry(currentEntry);
  }

  @Override
  public final String tokenForEntry(E entry) {
    return source.tokenForEntry(entry);
  }

  @Override
  public final void close() {}
}
