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
import org.projectnessie.api.params.NamespaceUpdate;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.ImmutableGetNamespacesResponse;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Ref;
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
  public Namespace createNamespace(NamespaceParams params, Namespace namespace)
      throws NessieNamespaceAlreadyExistsException, NessieReferenceNotFoundException {
    try {
      BranchName branch = branchFromRefName(params.getRefName());

      Callable<Void> validator =
          () -> {
            Optional<Content> explicitlyCreatedNamespace =
                getExplicitlyCreatedNamespace(namespace, branch);
            if (explicitlyCreatedNamespace.isPresent()) {
              Namespace ignored =
                  explicitlyCreatedNamespace
                      .get()
                      .unwrap(Namespace.class)
                      .orElseThrow(() -> otherContentAlreadyExistsException(namespace));
              throw namespaceAlreadyExistsException(namespace);
            }
            if (getImplicitlyCreatedNamespace(namespace, branch).isPresent()) {
              throw namespaceAlreadyExistsException(namespace);
            }
            return null;
          };

      Preconditions.checkArgument(!namespace.isEmpty(), "Namespace name must not be empty");

      ContentKey key = ContentKey.of(namespace.getElements());
      Put put = Put.of(key, namespace);
      Hash hash =
          commit(branch, "create namespace " + namespace.name(), TreeApiImpl.toOp(put), validator);

      Content content = getExplicitlyCreatedNamespace(namespace, hash).orElse(null);

      Preconditions.checkState(
          content instanceof Namespace,
          "Expected %s to return the created Namespace, but got %s",
          key,
          content);

      return (Namespace) content;
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
      Namespace namespace = getNamespace(params.getNamespace(), branch);
      Delete delete = Delete.of(ContentKey.of(namespace.getElements()));

      Callable<Void> validator =
          () -> {
            try (Stream<KeyEntry<Content.Type>> keys = getStore().getKeys(branch)) {
              if (keys.anyMatch(
                  k ->
                      Namespace.of(k.getKey().getElements())
                              .isSameOrSubElementOf(params.getNamespace())
                          && k.getType() != Content.Type.NAMESPACE)) {
                throw namespaceNotEmptyException(params.getNamespace());
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
      return getNamespace(params.getNamespace(), branchFromRefName(params.getRefName()));
    } catch (ReferenceNotFoundException e) {
      throw refNotFoundException(e);
    }
  }

  /**
   * First tries to look whether a namespace with the given name was explicitly created via {@link
   * NamespaceApi#createNamespace(NamespaceParams, Namespace)}, and then checks if there is an
   * implicit namespace. An implicitly created namespace generally occurs when adding a table
   * 'a.b.c.table' where the 'a.b.c' part * represents the namespace.
   *
   * @param namespace The namespace to fetch
   * @param branch The ref to use
   * @return A {@link Namespace} instance
   * @throws ReferenceNotFoundException If the ref could not be found
   * @throws NessieNamespaceNotFoundException If the namespace could not be found
   */
  private Namespace getNamespace(Namespace namespace, BranchName branch)
      throws ReferenceNotFoundException, NessieNamespaceNotFoundException {
    Optional<Content> explicitlyCreatedNamespace = getExplicitlyCreatedNamespace(namespace, branch);
    if (explicitlyCreatedNamespace.isPresent()) {
      return explicitlyCreatedNamespace
          .get()
          .unwrap(Namespace.class)
          .orElseThrow(() -> namespaceDoesNotExistException(namespace));
    }

    return getImplicitlyCreatedNamespace(namespace, branch)
        .orElseThrow(() -> namespaceDoesNotExistException(namespace));
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
      try (Stream<KeyEntry<Content.Type>> stream =
          getNamespacesKeyStream(params.getNamespace(), branch, k -> true)) {
        stream.forEach(
            namespaceKeyWithType -> {
              if (namespaceKeyWithType.getType() == Content.Type.NAMESPACE) {
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

  @Override
  public void updateProperties(NamespaceParams params, NamespaceUpdate namespaceUpdate)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    try {
      BranchName branch = branchFromRefName(params.getRefName());

      Namespace namespace = getNamespace(params.getNamespace(), branch);
      Map<String, String> properties = new HashMap<>(namespace.getProperties());
      if (null != namespaceUpdate.getPropertyRemovals()) {
        namespaceUpdate.getPropertyRemovals().forEach(properties::remove);
      }
      if (null != namespaceUpdate.getPropertyUpdates()) {
        properties.putAll(namespaceUpdate.getPropertyUpdates());
      }

      Namespace updatedNamespace = ImmutableNamespace.copyOf(namespace).withProperties(properties);

      Put put = Put.of(ContentKey.of(updatedNamespace.getElements()), updatedNamespace);
      commit(
          branch,
          "update properties for namespace " + updatedNamespace.name(),
          TreeApiImpl.toOp(put),
          () -> null);

    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  private Stream<KeyEntry<Content.Type>> getNamespacesKeyStream(
      @Nullable Namespace namespace,
      BranchName branch,
      Predicate<KeyEntry<Content.Type>> earlyFilterPredicate)
      throws ReferenceNotFoundException {
    return getStore()
        .getKeys(branch)
        .filter(earlyFilterPredicate)
        .filter(k -> null == namespace || namespaceFromType(k).isSameOrSubElementOf(namespace));
  }

  /**
   * If the {@link Content.Type} is an actual {@link Content.Type#NAMESPACE}, then we're returning
   * its name without modification as a {@link Namespace} instance. If the {@link Content.Type} is
   * for example {@link Content.Type#ICEBERG_TABLE} / {@link Content.Type#ICEBERG_VIEW} / {@link
   * Content.Type#DELTA_LAKE_TABLE}, then we are extracting its namespace name from the elements of
   * {@link Key} without including the actual table name itself (which is the last element).
   *
   * @param withType The {@link WithType} instance holding the key and type.
   * @return A {@link Namespace} instance.
   */
  private Namespace namespaceFromType(KeyEntry<Content.Type> withType) {
    List<String> elements = withType.getKey().getElements();
    if (Content.Type.NAMESPACE != withType.getType()) {
      elements = elements.subList(0, elements.size() - 1);
    }
    return Namespace.of(elements);
  }

  private Optional<Content> getExplicitlyCreatedNamespace(Namespace namespace, Ref ref)
      throws ReferenceNotFoundException {
    return Optional.ofNullable(getStore().getValue(ref, Key.of(namespace.getElements())));
  }

  private Optional<Namespace> getImplicitlyCreatedNamespace(Namespace namespace, BranchName branch)
      throws ReferenceNotFoundException {
    try (Stream<KeyEntry<Content.Type>> stream =
        getNamespacesKeyStream(namespace, branch, k -> true)) {
      return stream.findAny().map(this::namespaceFromType);
    }
  }

  private NessieNamespaceAlreadyExistsException namespaceAlreadyExistsException(
      Namespace namespace) {
    return new NessieNamespaceAlreadyExistsException(
        String.format("Namespace '%s' already exists", namespace));
  }

  private NessieNamespaceAlreadyExistsException otherContentAlreadyExistsException(
      Namespace namespace) {
    return new NessieNamespaceAlreadyExistsException(
        String.format("Another content object with name '%s' already exists", namespace));
  }

  private NessieNamespaceNotFoundException namespaceDoesNotExistException(Namespace namespace) {
    return new NessieNamespaceNotFoundException(
        String.format("Namespace '%s' does not exist", namespace));
  }

  private NessieNamespaceNotEmptyException namespaceNotEmptyException(Namespace namespace) {
    return new NessieNamespaceNotEmptyException(
        String.format("Namespace '%s' is not empty", namespace));
  }

  private BranchName branchFromRefName(String refName) {
    return BranchName.of(
        Optional.ofNullable(refName).orElseGet(() -> getConfig().getDefaultBranch()));
  }

  private NessieReferenceNotFoundException refNotFoundException(ReferenceNotFoundException e) {
    return new NessieReferenceNotFoundException(e.getMessage(), e);
  }

  private Hash commit(
      BranchName branch,
      String commitMsg,
      Operation<Content> contentOperation,
      Callable<Void> validator)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return getStore()
        .commit(
            branch,
            Optional.empty(),
            commitMetaUpdate().rewriteSingle(CommitMeta.fromMessage(commitMsg)),
            Collections.singletonList(contentOperation),
            validator);
  }
}
