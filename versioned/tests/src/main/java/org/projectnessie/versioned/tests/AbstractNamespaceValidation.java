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
package org.projectnessie.versioned.tests;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.Conflict.ConflictType.NAMESPACE_ABSENT;
import static org.projectnessie.model.Conflict.ConflictType.NAMESPACE_NOT_EMPTY;
import static org.projectnessie.model.Conflict.ConflictType.NOT_A_NAMESPACE;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.TransplantOp;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractNamespaceValidation extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractNamespaceValidation(VersionStore store) {
    super(store);
  }

  static Stream<ContentKey> contentKeys() {
    return Stream.of(ContentKey.of("ns", "table"), ContentKey.of("ns2", "ns", "table"));
  }

  @ParameterizedTest
  @MethodSource("contentKeys")
  void commitWithNonExistingNamespace(ContentKey key) throws Exception {
    assumeTrue(
        key.getElementCount() == 2,
        "multiple missing namespaces are tested separately below for the new storage");

    BranchName branch = BranchName.of("commitWithNonExistingNamespace");
    store().create(branch, Optional.empty());

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        fromMessage("non-existing-ns"),
                        singletonList(Put.of(key, newOnRef("value")))))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage("Namespace '%s' must exist.", key.getNamespace())
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(
                NAMESPACE_ABSENT,
                key.getNamespace().toContentKey(),
                "namespace '" + key.getNamespace() + "' must exist"));

    store()
        .commit(
            branch,
            Optional.empty(),
            fromMessage("initial commit"),
            singletonList(Put.of(ContentKey.of("unrelated-table"), newOnRef("value"))));

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        fromMessage("non-existing-ns"),
                        singletonList(Put.of(key, newOnRef("value")))))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage("Namespace '%s' must exist.", key.getNamespace())
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(
                NAMESPACE_ABSENT,
                key.getNamespace().toContentKey(),
                "namespace '" + key.getNamespace() + "' must exist"));
  }

  @Test
  void commitWithNonExistingNamespaceMultiple() throws Exception {
    ContentKey a = ContentKey.of("a");
    ContentKey b = ContentKey.of("a", "b");
    ContentKey c = ContentKey.of("a", "b", "c");
    ContentKey table = ContentKey.of("a", "b", "c", "table");
    BranchName branch = BranchName.of("commitWithNonExistingNamespace");
    store().create(branch, Optional.empty());

    store()
        .commit(
            branch,
            Optional.empty(),
            fromMessage("create ns a"),
            singletonList(Put.of(a, Namespace.of(a))));

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        fromMessage("non-existing-ns"),
                        singletonList(Put.of(table, newOnRef("value")))))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage(
            "There are multiple conflicts that prevent committing the provided operations: "
                + "namespace 'a.b.c' must exist, "
                + "namespace 'a.b' must exist.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(NAMESPACE_ABSENT, c, "namespace 'a.b.c' must exist"),
            tuple(NAMESPACE_ABSENT, b, "namespace 'a.b' must exist"));

    store()
        .commit(
            branch,
            Optional.empty(),
            fromMessage("create content b"),
            singletonList(Put.of(b, newOnRef("value"))));

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        fromMessage("non-existing-ns"),
                        singletonList(Put.of(table, newOnRef("value")))))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage(
            "There are multiple conflicts that prevent committing the provided operations: "
                + "namespace 'a.b.c' must exist, "
                + "expecting the key 'a.b' to be a namespace, but is not a namespace (using a content object that is not a namespace as a namespace is forbidden).")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(NAMESPACE_ABSENT, c, "namespace 'a.b.c' must exist"),
            tuple(
                NOT_A_NAMESPACE,
                b,
                "expecting the key 'a.b' to be a namespace, but is not a namespace (using a content object that is not a namespace as a namespace is forbidden)"));

    store()
        .commit(
            branch, Optional.empty(), fromMessage("delete content b"), singletonList(Delete.of(b)));
    store()
        .commit(
            branch, Optional.empty(), fromMessage("delete content a"), singletonList(Delete.of(a)));

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        fromMessage("non-existing-ns"),
                        singletonList(Put.of(table, newOnRef("value")))))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage(
            "There are multiple conflicts that prevent committing the provided operations: "
                + "namespace 'a.b.c' must exist, "
                + "namespace 'a.b' must exist, "
                + "namespace 'a' must exist.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(NAMESPACE_ABSENT, c, "namespace 'a.b.c' must exist"),
            tuple(NAMESPACE_ABSENT, b, "namespace 'a.b' must exist"),
            tuple(NAMESPACE_ABSENT, a, "namespace 'a' must exist"));
  }

  @ParameterizedTest
  @MethodSource("contentKeys")
  void commitWithNonNamespace(ContentKey key) throws Exception {
    BranchName branch = BranchName.of("commitWithNonNamespace");
    store().create(branch, Optional.empty());

    if (key.getElementCount() == 3) {
      // Give the non-namespace content commit a valid namespace.
      store()
          .commit(
              branch,
              Optional.empty(),
              fromMessage("initial commit"),
              singletonList(
                  Put.of(key.getParent().getParent(), Namespace.of(key.getParent().getParent()))));
    }

    // Add a non-namespace content using the parent key of the namespace to be checked below.
    store()
        .commit(
            branch,
            Optional.empty(),
            fromMessage("not a namespace"),
            singletonList(Put.of(key.getParent(), newOnRef("value"))));

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        fromMessage("non-existing-ns"),
                        singletonList(Put.of(key, newOnRef("value")))))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage(
            "Expecting the key '%s' to be a namespace, but is not a namespace "
                + "(using a content object that is not a namespace as a namespace is forbidden).",
            key.getNamespace())
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(
            tuple(
                NOT_A_NAMESPACE,
                key.getNamespace().toContentKey(),
                "expecting the key '"
                    + key.getNamespace()
                    + "' to be a namespace, but is not a namespace "
                    + "(using a content object that is not a namespace as a namespace is forbidden)"));
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void preventNamespaceDeletionWithChildren(boolean childNamespace) throws Exception {
    BranchName branch = BranchName.of("branch");
    store().create(branch, Optional.empty());

    Namespace ns = Namespace.of("ns");

    store()
        .commit(
            branch,
            Optional.empty(),
            fromMessage("initial"),
            asList(
                Put.of(ns.toContentKey(), ns),
                Put.of(ContentKey.of(ns, "table"), newOnRef("foo"))));

    if (childNamespace) {
      Namespace child = Namespace.of("ns", "child");
      store()
          .commit(
              branch,
              Optional.empty(),
              fromMessage("child ns"),
              singletonList(Put.of(child.toContentKey(), child)));
    }

    soft.assertThatThrownBy(
            () ->
                store.commit(
                    branch,
                    Optional.empty(),
                    fromMessage("try delete ns"),
                    singletonList(Delete.of(ns.toContentKey()))))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage("Namespace 'ns' is not empty.")
        .asInstanceOf(type(ReferenceConflictException.class))
        .extracting(ReferenceConflictException::getReferenceConflicts)
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .singleElement()
        .extracting(Conflict::conflictType, Conflict::key, Conflict::message)
        .containsExactly(NAMESPACE_NOT_EMPTY, ns.toContentKey(), "namespace 'ns' is not empty");
  }

  enum NamespaceValidationMergeTransplant {
    MERGE(true, false, false, false),
    MERGE_CREATE(true, true, false, false),
    MERGE_DELETE(true, false, true, true),
    TRANSPLANT(false, false, false, false),
    TRANSPLANT_CREATE(true, true, false, false),
    TRANSPLANT_DELETE(false, false, true, true),
    ;

    /** Whether to merge (or transplant, if false). */
    final boolean merge;

    /** Whether the namespace shall be created on the target branch. */
    final boolean createNamespaceOnTarget;

    /**
     * Whether the namespace shall be deleted on the target branch to trigger an error by the
     * namespace-exists check.
     */
    final boolean deleteNamespaceOnTarget;

    final boolean error;

    NamespaceValidationMergeTransplant(
        boolean merge,
        boolean createNamespaceOnTarget,
        boolean deleteNamespaceOnTarget,
        boolean error) {
      this.merge = merge;
      this.createNamespaceOnTarget = createNamespaceOnTarget;
      this.deleteNamespaceOnTarget = deleteNamespaceOnTarget;
      this.error = error;
    }
  }

  /**
   * Validate various combinations of merge/transplant scenarios, validating that the
   * "namespace-exists checks for merged/transplanted keys" works properly.
   *
   * @see NamespaceValidationMergeTransplant
   */
  @ParameterizedTest
  @EnumSource(NamespaceValidationMergeTransplant.class)
  void mergeTransplantWithCommonButRemovedNamespace(NamespaceValidationMergeTransplant mode)
      throws Exception {
    BranchName root = BranchName.of("root");
    store().create(root, Optional.empty());

    Namespace ns = Namespace.of("ns");
    Namespace ns2 = Namespace.of("ns2");
    CommitResult rootHead =
        store()
            .commit(
                root,
                Optional.empty(),
                fromMessage("create namespace"),
                mode.createNamespaceOnTarget
                    ? singletonList(Put.of(ns2.toContentKey(), ns2))
                    : asList(Put.of(ns.toContentKey(), ns), Put.of(ns2.toContentKey(), ns2)));

    BranchName branch = BranchName.of("branch");
    store().create(branch, Optional.of(rootHead.getCommitHash()));

    if (mode.createNamespaceOnTarget) {
      store()
          .commit(
              branch,
              Optional.empty(),
              fromMessage("create namespace"),
              singletonList(Put.of(ns.toContentKey(), ns)));
    }

    ContentKey key = ContentKey.of(ns, "foo");
    CommitResult commit1 =
        store()
            .commit(
                branch,
                Optional.empty(),
                fromMessage("create table ns.foo"),
                singletonList(Put.of(key, newOnRef("foo"))));

    CommitResult commit2 =
        store()
            .commit(
                branch,
                Optional.empty(),
                fromMessage("create table ns2.bar"),
                singletonList(Put.of(ContentKey.of(ns2, "bar"), newOnRef("bar"))));

    store()
        .commit(
            root,
            Optional.empty(),
            fromMessage("unrelated"),
            singletonList(Put.of(ContentKey.of("unrelated-table"), newOnRef("bar"))));

    ThrowingCallable mergeTransplant =
        mode.merge
            ? () ->
                store()
                    .merge(
                        VersionStore.MergeOp.builder()
                            .fromRef(branch)
                            .fromHash(
                                store().hashOnReference(branch, Optional.empty(), emptyList()))
                            .toBranch(root)
                            .build())
            : () ->
                store()
                    .transplant(
                        TransplantOp.builder()
                            .fromRef(branch)
                            .toBranch(root)
                            .addSequenceToTransplant(
                                commit1.getCommitHash(), commit2.getCommitHash())
                            .build());

    if (mode.deleteNamespaceOnTarget) {
      store()
          .commit(
              root,
              Optional.empty(),
              fromMessage("delete namespace"),
              singletonList(Delete.of(ns.toContentKey())));
    }

    if (mode.error) {
      soft.assertThatThrownBy(mergeTransplant)
          .isInstanceOf(ReferenceConflictException.class)
          .hasMessage("Namespace '%s' must exist.", key.getNamespace());
    } else {
      soft.assertThatCode(mergeTransplant).doesNotThrowAnyException();
    }
  }

  @Test
  void mustNotOverwriteNamespace() throws Exception {
    BranchName root = BranchName.of("root");
    store().create(root, Optional.empty());

    ContentKey key = ContentKey.of("key");

    store()
        .commit(
            root,
            Optional.empty(),
            fromMessage("create table ns.foo"),
            singletonList(Put.of(key, Namespace.of(key))));

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        root,
                        Optional.empty(),
                        fromMessage("create table ns.foo"),
                        singletonList(Put.of(key, newOnRef("foo")))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void deleteHierarchy() throws Exception {
    BranchName root = BranchName.of("root");
    store().create(root, Optional.empty());

    List<Namespace> namespaces =
        asList(
            Namespace.of("a"),
            Namespace.of("a", "b"),
            Namespace.of("a", "b", "c"),
            Namespace.of("x"),
            Namespace.of("x", "y"),
            Namespace.of("x", "y", "z"));
    List<ContentKey> tables =
        namespaces.stream()
            .flatMap(
                ns ->
                    Stream.of(
                        ContentKey.of(ns, "A"), ContentKey.of(ns, "B"), ContentKey.of(ns, "C")))
            .collect(Collectors.toList());

    store()
        .commit(
            root,
            Optional.empty(),
            fromMessage("unrelated"),
            Stream.concat(
                    tables.stream().map(t -> Put.of(t, newOnRef(t.toString()))),
                    namespaces.stream().map(ns -> Put.of(ns.toContentKey(), ns)))
                .collect(Collectors.toList()));

    soft.assertThatCode(
            () ->
                store()
                    .commit(
                        root,
                        Optional.empty(),
                        fromMessage("delete all the things"),
                        Stream.concat(
                                namespaces.stream().map(ns -> Delete.of(ns.toContentKey())),
                                tables.stream().map(Delete::of))
                            .collect(Collectors.toList())))
        .doesNotThrowAnyException();
  }

  @Test
  void renameWithNonExistingNamespace() throws Exception {
    BranchName branch = BranchName.of("renameWithNonExistingNamespace");
    store().create(branch, Optional.empty());

    ContentKey key1 = ContentKey.of("table");
    ContentKey key2 = ContentKey.of(Namespace.of("non_existing"), "tbl");

    store()
        .commit(
            branch,
            Optional.empty(),
            fromMessage("create a table"),
            singletonList(Put.of(key1, newOnRef("value"))));

    Content table = requireNonNull(store().getValue(branch, key1, false).content());

    soft.assertThatThrownBy(
            () ->
                store()
                    .commit(
                        branch,
                        Optional.empty(),
                        fromMessage("rename table"),
                        asList(Delete.of(key1), Put.of(key2, table))))
        .isInstanceOf(ReferenceConflictException.class)
        .hasMessage("Namespace 'non_existing' must exist.");
  }
}
