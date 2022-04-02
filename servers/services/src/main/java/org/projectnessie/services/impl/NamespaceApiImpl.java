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
package org.projectnessie.services.impl;

import com.google.common.base.Preconditions;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.projectnessie.api.NamespaceApi;
import org.projectnessie.api.params.MultipleNamespacesParams;
import org.projectnessie.api.params.NamespaceParams;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.ImmutableGetNamespacesResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithType;

public class NamespaceApiImpl extends BaseApiImpl implements NamespaceApi {

  public NamespaceApiImpl(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Content.Type> store,
      Authorizer authorizer,
      Principal principal) {
    super(config, store, authorizer, principal);
  }

  @Override
  public Namespace createNamespace(NamespaceParams params)
      throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException {
    try {
      BranchName branch = branchFromRefName(params.getRefName());

      Callable<Void> validator =
          () -> {
            Optional<Content> explicitlyCreatedNamespace =
                getExplicitlyCreatedNamespace(params, branch);
            if (explicitlyCreatedNamespace.isPresent()) {
              Namespace ignored =
                  explicitlyCreatedNamespace
                      .get()
                      .unwrap(Namespace.class)
                      .orElseThrow(() -> otherContentAlreadyExistsException(params));
              throw namespaceAlreadyExistsException(params);
            }
            if (getImplicitlyCreatedNamespace(params, branch).isPresent()) {
              throw namespaceAlreadyExistsException(params);
            }
            return null;
          };

      Namespace namespace = params.getNamespace();
      Preconditions.checkArgument(!namespace.isEmpty(), "Namespace name must not be empty");
      Put put = Put.of(ContentKey.of(namespace.getElements()), namespace);
      commit(branch, "create namespace " + namespace.name(), TreeApiImpl.toOp(put), validator);

      return namespace;
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  @Override
  public void deleteNamespace(NamespaceParams params)
      throws NessieReferenceNotFoundException, NessieNamespaceNotEmptyException,
          NessieNamespaceNotFoundException {
    BranchName branch = branchFromRefName(params.getRefName());
    try {
      Namespace namespace = getNamespace(params, branch);
      Delete delete = Delete.of(ContentKey.of(namespace.getElements()));

      Callable<Void> validator =
          () -> {
            try (Stream<KeyEntry<Type>> keys = getStore().getKeys(branch)) {
              if (keys.anyMatch(
                  k ->
                      Namespace.of(k.getKey().getElements())
                              .name()
                              .startsWith(params.getNamespace().name())
                          && k.getType() != Type.NAMESPACE)) {
                throw namespaceNotEmptyException(params);
              }
            }
            return null;
          };

      commit(branch, "delete namespace " + namespace.name(), TreeApiImpl.toOp(delete), validator);
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  @Override
  public Namespace getNamespace(NamespaceParams params)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    try {
      return getNamespace(params, branchFromRefName(params.getRefName()));
    } catch (ReferenceNotFoundException e) {
      throw refNotFoundException(e);
    }
  }

  /**
   * First tries to look whether a namespace with the given name was explicitly created via {@link
   * NamespaceApi#createNamespace(NamespaceParams)}, and then checks if there is an implicit
   * namespace. An implicitly created namespace generally occurs when adding a table 'a.b.c.table'
   * where the 'a.b.c' part * represents the namespace.
   *
   * @param params The params to use for the fetching the namespace
   * @param branch The ref to use
   * @return A {@link Namespace} instance
   * @throws ReferenceNotFoundException If the ref could not be found
   * @throws NessieNamespaceNotFoundException If the namespace could not be found
   */
  private Namespace getNamespace(NamespaceParams params, BranchName branch)
      throws ReferenceNotFoundException, NessieNamespaceNotFoundException {
    Optional<Content> explicitlyCreatedNamespace = getExplicitlyCreatedNamespace(params, branch);
    if (explicitlyCreatedNamespace.isPresent()) {
      return explicitlyCreatedNamespace
          .get()
          .unwrap(Namespace.class)
          .orElseThrow(() -> namespaceDoesNotExistException(params));
    }

    return getImplicitlyCreatedNamespace(params, branch)
        .orElseThrow(() -> namespaceDoesNotExistException(params));
  }

  @Override
  public GetNamespacesResponse getNamespaces(MultipleNamespacesParams params)
      throws NessieReferenceNotFoundException {
    BranchName branch = branchFromRefName(params.getRefName());
    try {

      // Note: `Namespace` objects are supposed to get more attributes (e.g. a properties map)
      // which will make it impossible to use the `Namespace` object itself as an identifier to
      // subtract the set of explicitly created namespaces from the set of implicitly created ones.

      // Iterate through all candidate keys, split into `Key`s of explicitly created namespaces
      // (type==NAMESPACE) and collect implicitly created namespaces for all other content-types.
      Set<Key> explicitNamespaceKeys = new HashSet<>();
      Map<List<String>, Namespace> implicitNamespaces = new HashMap<>();
      try (Stream<KeyEntry<Type>> stream =
          getNamespacesKeyStream(params.getNamespace(), branch, k -> true)) {
        stream.forEach(
            namespaceKeyWithType -> {
              if (namespaceKeyWithType.getType() == Type.NAMESPACE) {
                explicitNamespaceKeys.add(namespaceKeyWithType.getKey());
              } else {
                Namespace implicitNamespace = namespaceFromType(namespaceKeyWithType);
                if (!implicitNamespace.isEmpty()) {
                  implicitNamespaces.put(implicitNamespace.getElements(), implicitNamespace);
                }
              }
            });
      }

      ImmutableGetNamespacesResponse.Builder response = ImmutableGetNamespacesResponse.builder();

      // Next step: fetch the content-objects (of type `Namespace`) for all collected explicit
      // namespaces, add those to the response and the implicitly created `Namespace` for the
      // same key.
      if (!explicitNamespaceKeys.isEmpty()) {
        Map<Key, Content> namespaceValues = getStore().getValues(branch, explicitNamespaceKeys);
        namespaceValues.values().stream()
            .filter(Namespace.class::isInstance)
            .map(Namespace.class::cast)
            .peek(explicitNamepsace -> implicitNamespaces.remove(explicitNamepsace.getElements()))
            .forEach(response::addNamespaces);
      }

      // Add the remaining (= those not being explicitly created) implicitly created namespaces
      // to the response.
      response.addAllNamespaces(implicitNamespaces.values());

      return response.build();
    } catch (ReferenceNotFoundException e) {
      throw refNotFoundException(e);
    }
  }

  private Stream<KeyEntry<Type>> getNamespacesKeyStream(
      @Nullable Namespace namespace,
      BranchName branch,
      Predicate<KeyEntry<Type>> earlyFilterPredicate)
      throws ReferenceNotFoundException {
    return getStore()
        .getKeys(branch)
        .filter(earlyFilterPredicate)
        .filter(k -> null == namespace || namespaceFromType(k).name().startsWith(namespace.name()));
  }

  /**
   * If the {@link Type} is an actual {@link Type#NAMESPACE}, then we're returning its name without
   * modification as a {@link Namespace} instance. If the {@link Type} is for example {@link
   * Type#ICEBERG_TABLE} / {@link Type#ICEBERG_VIEW} / {@link Type#DELTA_LAKE_TABLE}, then we are
   * extracting its namespace name from the elements of {@link Key} without including the actual
   * table name itself (which is the last element).
   *
   * @param withType The {@link WithType} instance holding the key and type.
   * @return A {@link Namespace} instance.
   */
  private Namespace namespaceFromType(KeyEntry<Type> withType) {
    List<String> elements = withType.getKey().getElements();
    if (Type.NAMESPACE != withType.getType()) {
      elements = elements.subList(0, elements.size() - 1);
    }
    return Namespace.of(elements);
  }

  private Optional<Content> getExplicitlyCreatedNamespace(NamespaceParams params, BranchName branch)
      throws ReferenceNotFoundException {
    return Optional.ofNullable(
        getStore().getValue(branch, Key.of(params.getNamespace().getElements())));
  }

  private Optional<Namespace> getImplicitlyCreatedNamespace(
      NamespaceParams params, BranchName branch) throws ReferenceNotFoundException {
    try (Stream<KeyEntry<Type>> stream =
        getNamespacesKeyStream(params.getNamespace(), branch, k -> true)) {
      return stream.findAny().map(this::namespaceFromType);
    }
  }

  private NessieNamespaceAlreadyExistsException namespaceAlreadyExistsException(
      NamespaceParams params) {
    return new NessieNamespaceAlreadyExistsException(
        String.format("Namespace '%s' already exists", params.getNamespace()));
  }

  private NessieNamespaceAlreadyExistsException otherContentAlreadyExistsException(
      NamespaceParams params) {
    return new NessieNamespaceAlreadyExistsException(
        String.format(
            "Another content object with name '%s' already exists", params.getNamespace()));
  }

  private NessieNamespaceNotFoundException namespaceDoesNotExistException(NamespaceParams params) {
    return new NessieNamespaceNotFoundException(
        String.format("Namespace '%s' does not exist", params.getNamespace()));
  }

  private NessieNamespaceNotEmptyException namespaceNotEmptyException(NamespaceParams params) {
    return new NessieNamespaceNotEmptyException(
        String.format("Namespace '%s' is not empty", params.getNamespace()));
  }

  private BranchName branchFromRefName(String refName) {
    return BranchName.of(
        Optional.ofNullable(refName).orElseGet(() -> getConfig().getDefaultBranch()));
  }

  private NessieReferenceNotFoundException refNotFoundException(ReferenceNotFoundException e) {
    return new NessieReferenceNotFoundException(e.getMessage(), e);
  }

  private void commit(
      BranchName branch,
      String commitMsg,
      Operation<Content> contentOperation,
      Callable<Void> validator)
      throws ReferenceNotFoundException, ReferenceConflictException {
    getStore()
        .commit(
            branch,
            Optional.empty(),
            commitMetaUpdateSingle().apply(CommitMeta.fromMessage(commitMsg)),
            Collections.singletonList(contentOperation),
            validator);
  }
}
