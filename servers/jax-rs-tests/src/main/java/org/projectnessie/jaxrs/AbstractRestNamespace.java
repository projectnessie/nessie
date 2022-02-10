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
package org.projectnessie.jaxrs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestNamespace extends AbstractRestRefLog {
  @Test
  public void testNamespaces() throws BaseNessieClientServerException {
    Branch branch = createBranch("testNamespaces");
    Namespace ns = Namespace.parse("a.b.c");
    Namespace namespace =
        getApi().createNamespace().refName(branch.getName()).namespace(ns).create();

    assertThat(namespace).isNotNull().isEqualTo(ns);
    assertThat(getApi().getNamespace().refName(branch.getName()).namespace(ns).get())
        .isEqualTo(namespace);

    assertThatThrownBy(
            () -> getApi().createNamespace().refName(branch.getName()).namespace(ns).create())
        .isInstanceOf(NessieNamespaceAlreadyExistsException.class)
        .hasMessage("Namespace 'a.b.c' already exists");

    getApi().deleteNamespace().refName(branch.getName()).namespace(ns).delete();
    assertThatThrownBy(
            () -> getApi().deleteNamespace().refName(branch.getName()).namespace(ns).delete())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'a.b.c' does not exist");

    assertThatThrownBy(() -> getApi().getNamespace().refName(branch.getName()).namespace(ns).get())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'a.b.c' does not exist");

    assertThatThrownBy(
            () ->
                getApi()
                    .deleteNamespace()
                    .refName(branch.getName())
                    .namespace(Namespace.parse("nonexisting"))
                    .delete())
        .isInstanceOf(NessieNamespaceNotFoundException.class)
        .hasMessage("Namespace 'nonexisting' does not exist");
  }

  @Test
  public void testNamespacesRetrieval() throws BaseNessieClientServerException {
    Branch branch = createBranch("testNamespacesRetrieval");
    Namespace one = Namespace.parse("a.b.c");
    Namespace two = Namespace.parse("a.b.d");
    Namespace three = Namespace.parse("x.y.z");
    Namespace four = Namespace.parse("one.two");
    for (Namespace namespace : Arrays.asList(one, two, three, four)) {
      assertThat(getApi().createNamespace().refName(branch.getName()).namespace(namespace).create())
          .isNotNull();
    }

    assertThat(
            getApi().getNamespaces().refName(branch.getName()).namespace("a").get().getNamespaces())
        .containsExactlyInAnyOrder(one, two);
    assertThat(
            getApi()
                .getNamespaces()
                .refName(branch.getName())
                .namespace("a.b")
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(one, two);
    assertThat(
            getApi()
                .getNamespaces()
                .refName(branch.getName())
                .namespace("a.b.c")
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(one);
    assertThat(
            getApi()
                .getNamespaces()
                .refName(branch.getName())
                .namespace("a.b.d")
                .get()
                .getNamespaces())
        .containsExactlyInAnyOrder(two);

    assertThat(
            getApi().getNamespaces().refName(branch.getName()).namespace("x").get().getNamespaces())
        .containsExactly(three);
    assertThat(
            getApi().getNamespaces().refName(branch.getName()).namespace("z").get().getNamespaces())
        .isEmpty();
    assertThat(
            getApi()
                .getNamespaces()
                .refName(branch.getName())
                .namespace("on")
                .get()
                .getNamespaces())
        .containsExactly(four);
  }

  @Test
  public void testNamespaceDeletion() throws BaseNessieClientServerException {
    Branch branch = createBranch("testNamespaceDeletion");

    CommitMultipleOperationsBuilder commit =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("verifyAllContentAndOperationTypes"));
    contentAndOperationTypes()
        .flatMap(
            c ->
                c.globalOperation == null
                    ? Stream.of(c.operation)
                    : Stream.of(c.operation, c.globalOperation))
        .forEach(commit::operation);
    commit.commit();

    List<Entry> entries =
        contentAndOperationTypes()
            .filter(c -> c.operation instanceof Put)
            .map(c -> Entry.builder().type(c.type).name(c.operation.getKey()).build())
            .collect(Collectors.toList());

    for (Entry e : entries) {
      Namespace namespace = e.getName().getNamespace();
      assertThat(getApi().getNamespace().refName(branch.getName()).namespace(namespace).get())
          .isEqualTo(namespace);

      assertThatThrownBy(
              () ->
                  getApi()
                      .deleteNamespace()
                      .refName(branch.getName())
                      .namespace(namespace)
                      .delete())
          .isInstanceOf(NessieNamespaceNotEmptyException.class)
          .hasMessage(String.format("Namespace '%s' is not empty", namespace));
    }
  }
}
