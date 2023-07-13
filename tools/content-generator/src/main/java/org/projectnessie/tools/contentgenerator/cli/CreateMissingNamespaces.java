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
package org.projectnessie.tools.contentgenerator.cli;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.projectnessie.model.Content.Type.NAMESPACE;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;

import com.google.common.annotations.VisibleForTesting;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.ColorScheme;
import picocli.CommandLine.Option;

/** Deletes content objects. */
@Command(
    name = "create-missing-namespaces",
    mixinStandardHelpOptions = true,
    description = "Creates missing namespaces for content keys at branch HEADs.")
public class CreateMissingNamespaces extends CommittingCommand {

  @Option(
      names = {"-r", "--branch"},
      description = "Name(s) of the branch(es) to process, defaults to all branches.")
  private List<String> branchNames = emptyList();

  @Override
  public void execute() throws NessieNotFoundException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      if (isVerbose()) {
        spec.commandLine().getOut().println("Start fetching and processing references...");
      }

      ColorScheme colorScheme = spec.commandLine().getColorScheme();

      Set<String> branchesNotFound = new HashSet<>(branchNames);
      AggregateResult result =
          branchesStream(api, n -> (branchNames.isEmpty() || branchesNotFound.remove(n)))
              .map(r -> processReference(api, r))
              .sorted(Comparator.comparing(r -> r.branch().getName()))
              .peek(
                  r ->
                      r.error()
                          .ifPresent(
                              err -> {
                                if (r.error().isPresent()) {
                                  spec.commandLine()
                                      .getErr()
                                      .println(
                                          format(
                                              "%s%n%s",
                                              colorScheme.errorText(
                                                  format(
                                                      "Failed to handle missing namespaces for branch %s",
                                                      r.branch())),
                                              colorScheme.stackTraceText(err)));
                                }
                              }))
              .map(AggregateResult::fromReferenceResult)
              .reduce(AggregateResult.zero(), AggregateResult::add);

      if (!branchNames.isEmpty() && !branchesNotFound.isEmpty()) {
        addError(
            "Could not find branch(es) %s specified as command line arguments.",
            branchesNotFound.stream().sorted().collect(Collectors.joining(", ")));
      }

      if (result.errors() == 0) {
        spec.commandLine()
            .getOut()
            .printf(
                "Successfully processed %d branches, created %d namespaces.%n",
                result.branches(), result.createdNamespaces());
      } else {
        addError(
            "Processed %d branches, %d branches failed, created %d namespaces.",
            result.branches(), result.errors(), result.createdNamespaces());
      }
    }
  }

  @Value.Immutable
  interface AggregateResult {
    @Value.Parameter(order = 1)
    int branches();

    @Value.Parameter(order = 2)
    int errors();

    @Value.Parameter(order = 3)
    int createdNamespaces();

    static AggregateResult fromReferenceResult(ReferenceResult referenceResult) {
      return ImmutableAggregateResult.of(
          1,
          referenceResult.error().isPresent() ? 1 : 0,
          referenceResult.createdNamespaces().size());
    }

    static AggregateResult zero() {
      return ImmutableAggregateResult.of(0, 0, 0);
    }

    default AggregateResult add(AggregateResult other) {
      return ImmutableAggregateResult.of(
          branches() + other.branches(),
          errors() + other.errors(),
          createdNamespaces() + other.createdNamespaces());
    }
  }

  @Value.Immutable
  interface ReferenceResult {
    Branch branch();

    List<ContentKey> createdNamespaces();

    Optional<Exception> error();
  }

  private ReferenceResult processReference(NessieApiV2 api, Branch branch) {
    if (isVerbose()) {
      spec.commandLine().getOut().printf("  processing branch %s...%n", branch.getName());
    }

    ImmutableReferenceResult.Builder result = ImmutableReferenceResult.builder().branch(branch);

    try {
      List<ContentKey> missingNamespaces = collectMissingNamespaceKeys(api, branch);

      if (!missingNamespaces.isEmpty()) {
        if (isVerbose()) {
          spec.commandLine()
              .getOut()
              .printf("    creating %d namespaces...%n", missingNamespaces.size());
          missingNamespaces.forEach(k -> spec.commandLine().getOut().printf("      - %s%n", k));
        }

        commitCreateNamespaces(api, branch, missingNamespaces);
        result.createdNamespaces(missingNamespaces);
        if (isVerbose()) {
          spec.commandLine().getOut().println("    ... committed.");
        }
      } else {
        if (isVerbose()) {
          spec.commandLine().getOut().println("    all namespaces present.");
        }
      }
    } catch (Exception e) {
      result.error(e);
    }
    return result.build();
  }

  @VisibleForTesting
  static Stream<Branch> branchesStream(NessieApiV2 api, Predicate<String> includeBranch)
      throws NessieNotFoundException {
    return api.getAllReferences().stream()
        .filter(r -> includeBranch.test(r.getName()))
        .filter(r -> r.getType() == BRANCH)
        .map(Branch.class::cast);
  }

  @VisibleForTesting
  void commitCreateNamespaces(NessieApiV2 api, Branch branch, List<ContentKey> missingNamespaces)
      throws NessieNotFoundException, NessieConflictException {
    CommitMultipleOperationsBuilder commit = api.commitMultipleOperations().branch(branch);
    for (ContentKey nsKey : missingNamespaces) {
      commit.operation(Put.of(nsKey, Namespace.of(nsKey)));
    }
    commit
        .commitMeta(
            commitMetaFromMessage(
                missingNamespaces.stream()
                    .map(ContentKey::toString)
                    .collect(Collectors.joining(", ", "Create namespaces ", ""))))
        .commit();
  }

  @VisibleForTesting
  static List<ContentKey> collectMissingNamespaceKeys(NessieApiV2 api, Branch branch)
      throws NessieNotFoundException {
    Set<ContentKey> existing = new HashSet<>();
    return api.getEntries().refName(branch.getName()).stream()
        .flatMap(
            e -> {
              ContentKey key = e.getName();
              if (e.getType() == NAMESPACE) {
                existing.add(key);
              }
              if (key.getElementCount() == 1) {
                return Stream.empty();
              }
              return IntStream.range(1, key.getElementCount())
                  .mapToObj(i -> ContentKey.of(key.getElements().subList(0, i)));
            })
        .sorted()
        .distinct()
        // The following 'filter' using the 'existing' collection is fine, because the previous
        // sorted-distinct triggers computation.
        .filter(k -> !existing.contains(k))
        .collect(Collectors.toList());
  }
}
