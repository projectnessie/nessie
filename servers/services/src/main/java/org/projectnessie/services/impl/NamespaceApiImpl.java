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

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.projectnessie.error.ContentKeyErrorDetails.contentKeyErrorDetails;
import static org.projectnessie.model.Validation.validateHash;
import static org.projectnessie.services.impl.RefUtil.toReference;
import static org.projectnessie.versioned.RequestMeta.API_WRITE;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.MustBeClosed;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.projectnessie.api.v1.NamespaceApi;
import org.projectnessie.api.v1.params.NamespaceParams;
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
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.hash.ResolvedHash;
import org.projectnessie.services.spi.NamespaceService;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.RequestMeta;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.paging.PaginationIterator;

public class NamespaceApiImpl extends BaseApiImpl implements NamespaceService {

  public NamespaceApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext,
      ApiContext apiContext) {
    super(config, store, authorizer, accessContext, apiContext);
  }

  @Override
  public Namespace createNamespace(String refName, Namespace namespace, RequestMeta requestMeta)
      throws NessieReferenceNotFoundException {
    Preconditions.checkArgument(!namespace.isEmpty(), "Namespace name must not be empty");

    try {
      ResolvedHash refWithHash = getHashResolver().resolveToHead(refName);

      try {
        Optional<Content> explicitlyCreatedNamespace =
            getExplicitlyCreatedNamespace(namespace, refWithHash.getHash());
        if (explicitlyCreatedNamespace.isPresent()) {
          Namespace ignored =
              explicitlyCreatedNamespace
                  .get()
                  .unwrap(Namespace.class)
                  .orElseThrow(() -> otherContentAlreadyExistsException(namespace));
          throw namespaceAlreadyExistsException(namespace);
        }
        if (getImplicitlyCreatedNamespace(namespace, refWithHash.getHash()).isPresent()) {
          throw namespaceAlreadyExistsException(namespace);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      ContentKey key = namespace.toContentKey();
      Put put = Put.of(key, namespace);
      Hash hash =
          commit(
              BranchName.of(refWithHash.getValue().getName()),
              "create namespace '" + namespace.toCanonicalString() + "'",
              put,
              requestMeta);

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
  public void deleteNamespace(String refName, Namespace namespaceToDelete)
      throws NessieReferenceNotFoundException, NessieNamespaceNotFoundException {
    try {
      ResolvedHash refWithHash = getHashResolver().resolveToHead(refName);
      Namespace namespace = getNamespace(namespaceToDelete, refWithHash.getHash());
      Delete delete = Delete.of(namespace.toContentKey());

      try (PaginationIterator<KeyEntry> keys =
          getStore().getKeys(refWithHash.getHash(), null, false, NO_KEY_RESTRICTIONS)) {
        while (keys.hasNext()) {
          KeyEntry k = keys.next();
          if (Namespace.of(k.getKey().contentKey().getElements())
                  .isSameOrSubElementOf(namespaceToDelete)
              && !Content.Type.NAMESPACE.equals(k.getKey().type())) {
            throw namespaceNotEmptyException(namespaceToDelete);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      commit(
          BranchName.of(refWithHash.getValue().getName()),
          "delete namespace '" + namespace.toCanonicalString() + "'",
          delete,
          API_WRITE);
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  @Override
  public Namespace getNamespace(String refName, String hashOnRef, Namespace namespace)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    try {
      if (hashOnRef != null) {
        validateHash(hashOnRef);
      }
      ResolvedHash resolved = getHashResolver().resolveHashOnRef(refName, hashOnRef);
      return getNamespace(namespace, resolved.getHash());
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
   * @param hash The hash to use
   * @return A {@link Namespace} instance
   * @throws ReferenceNotFoundException If the ref could not be found
   * @throws NessieNamespaceNotFoundException If the namespace could not be found
   */
  private Namespace getNamespace(Namespace namespace, Hash hash)
      throws ReferenceNotFoundException, NessieNamespaceNotFoundException {
    Optional<Content> explicitlyCreatedNamespace = getExplicitlyCreatedNamespace(namespace, hash);
    if (explicitlyCreatedNamespace.isPresent()) {
      return explicitlyCreatedNamespace
          .get()
          .unwrap(Namespace.class)
          .orElseThrow(() -> namespaceDoesNotExistException(namespace));
    }

    return getImplicitlyCreatedNamespace(namespace, hash)
        .orElseThrow(() -> namespaceDoesNotExistException(namespace));
  }

  @Override
  public GetNamespacesResponse getNamespaces(String refName, String hashOnRef, Namespace namespace)
      throws NessieReferenceNotFoundException {
    if (hashOnRef != null) {
      validateHash(hashOnRef);
    }
    try {
      ResolvedHash refWithHash = getHashResolver().resolveHashOnRef(refName, hashOnRef);
      // Note: `Namespace` objects are supposed to get more attributes (e.g. a properties map)
      // which will make it impossible to use the `Namespace` object itself as an identifier to
      // subtract the set of explicitly created namespaces from the set of implicitly created ones.

      // Iterate through all candidate keys, split into `Key`s of explicitly created namespaces
      // (type==NAMESPACE) and collect implicitly created namespaces for all other content-types.
      Set<ContentKey> explicitNamespaceKeys = new HashSet<>();
      Map<List<String>, Namespace> implicitNamespaces = new HashMap<>();
      try (Stream<KeyEntry> stream =
          getNamespacesKeyStream(namespace, refWithHash.getHash(), k -> true)) {
        stream.forEach(
            namespaceKeyWithType -> {
              if (Content.Type.NAMESPACE.equals(namespaceKeyWithType.getKey().type())) {
                explicitNamespaceKeys.add(namespaceKeyWithType.getKey().contentKey());
              } else {
                Namespace implicitNamespace = namespaceFromType(namespaceKeyWithType);
                if (!implicitNamespace.isEmpty()) {
                  implicitNamespaces.put(implicitNamespace.getElements(), implicitNamespace);
                }
              }
            });
      }

      ImmutableGetNamespacesResponse.Builder response =
          GetNamespacesResponse.builder()
              .effectiveReference(toReference(refWithHash.getValue(), refWithHash.getHash()));

      // Next step: fetch the content-objects (of type `Namespace`) for all collected explicit
      // namespaces, add those to the response and the implicitly created `Namespace` for the
      // same key.
      if (!explicitNamespaceKeys.isEmpty()) {
        Map<ContentKey, ContentResult> namespaceValues =
            getStore().getValues(refWithHash.getHash(), explicitNamespaceKeys, false);
        namespaceValues.values().stream()
            .map(ContentResult::content)
            .filter(Namespace.class::isInstance)
            .map(Namespace.class::cast)
            .peek(explicitNamespace -> implicitNamespaces.remove(explicitNamespace.getElements()))
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
  public void updateProperties(
      String refName,
      Namespace namespaceToUpdate,
      Map<String, String> propertyUpdates,
      Set<String> propertyRemovals,
      RequestMeta requestMeta)
      throws NessieNamespaceNotFoundException, NessieReferenceNotFoundException {
    try {
      ResolvedHash refWithHash = getHashResolver().resolveToHead(refName);

      Namespace namespace = getNamespace(namespaceToUpdate, refWithHash.getHash());
      Map<String, String> properties = new HashMap<>(namespace.getProperties());
      if (null != propertyRemovals) {
        propertyRemovals.forEach(properties::remove);
      }
      if (null != propertyUpdates) {
        properties.putAll(propertyUpdates);
      }

      Namespace updatedNamespace = ImmutableNamespace.copyOf(namespace).withProperties(properties);

      Put put = Put.of(updatedNamespace.toContentKey(), updatedNamespace);
      commit(
          BranchName.of(refWithHash.getValue().getName()),
          "update properties for namespace '" + updatedNamespace.toCanonicalString() + "'",
          put,
          requestMeta);

    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }

  @MustBeClosed
  private Stream<KeyEntry> getNamespacesKeyStream(
      @Nullable Namespace namespace, Hash hash, Predicate<KeyEntry> earlyFilterPredicate)
      throws ReferenceNotFoundException {
    PaginationIterator<KeyEntry> iter = getStore().getKeys(hash, null, false, NO_KEY_RESTRICTIONS);
    return stream(spliteratorUnknownSize(iter, 0), false)
        .onClose(iter::close)
        .filter(earlyFilterPredicate)
        .filter(k -> null == namespace || namespaceFromType(k).isSameOrSubElementOf(namespace));
  }

  /**
   * If the {@link Content.Type} is an actual {@link Content.Type#NAMESPACE}, then we're returning
   * its name without modification as a {@link Namespace} instance. If the {@link Content.Type} is
   * for example {@link Content.Type#ICEBERG_TABLE} / {@link Content.Type#ICEBERG_VIEW} / {@link
   * Content.Type#DELTA_LAKE_TABLE}, then we are extracting its namespace name from the elements of
   * {@link ContentKey} without including the actual table name itself (which is the last element).
   *
   * @param withType The {@link KeyEntry} instance holding the key and type.
   * @return A {@link Namespace} instance.
   */
  private static Namespace namespaceFromType(KeyEntry withType) {
    List<String> elements = withType.getKey().contentKey().getElements();
    if (!Content.Type.NAMESPACE.equals(withType.getKey().type())) {
      elements = elements.subList(0, elements.size() - 1);
    }
    return Namespace.of(elements);
  }

  private Optional<Content> getExplicitlyCreatedNamespace(Namespace namespace, Hash hash)
      throws ReferenceNotFoundException {
    return Optional.ofNullable(getStore().getValue(hash, namespace.toContentKey(), false))
        .map(ContentResult::content);
  }

  private Optional<Namespace> getImplicitlyCreatedNamespace(Namespace namespace, Hash hash)
      throws ReferenceNotFoundException {
    try (Stream<KeyEntry> stream = getNamespacesKeyStream(namespace, hash, k -> true)) {
      return stream.findAny().map(NamespaceApiImpl::namespaceFromType);
    }
  }

  private static NessieNamespaceAlreadyExistsException namespaceAlreadyExistsException(
      Namespace namespace) {
    return new NessieNamespaceAlreadyExistsException(
        contentKeyErrorDetails(namespace.toContentKey()),
        String.format("Namespace '%s' already exists", namespace));
  }

  private static NessieNamespaceAlreadyExistsException otherContentAlreadyExistsException(
      Namespace namespace) {
    return new NessieNamespaceAlreadyExistsException(
        contentKeyErrorDetails(namespace.toContentKey()),
        String.format("Another content object with name '%s' already exists", namespace));
  }

  private static NessieNamespaceNotFoundException namespaceDoesNotExistException(
      Namespace namespace) {
    return new NessieNamespaceNotFoundException(
        contentKeyErrorDetails(namespace.toContentKey()),
        String.format("Namespace '%s' does not exist", namespace));
  }

  private static NessieNamespaceNotEmptyException namespaceNotEmptyException(Namespace namespace) {
    return new NessieNamespaceNotEmptyException(
        contentKeyErrorDetails(namespace.toContentKey()),
        String.format("Namespace '%s' is not empty", namespace));
  }

  private static NessieReferenceNotFoundException refNotFoundException(
      ReferenceNotFoundException e) {
    return new NessieReferenceNotFoundException(e.getMessage(), e);
  }

  private Hash commit(
      BranchName branch, String commitMsg, Operation contentOperation, RequestMeta requestMeta)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return getStore()
        .commit(
            branch,
            Optional.empty(),
            commitMetaUpdate(null, numCommits -> null)
                .rewriteSingle(CommitMeta.fromMessage(commitMsg)),
            Collections.singletonList(contentOperation),
            validation -> {
              BatchAccessChecker check = startAccessCheck().canCommitChangeAgainstReference(branch);
              validation
                  .operations()
                  .forEach(
                      op -> {
                        Set<String> actions =
                            requestMeta.keyActions(op.identifiedKey().contentKey());
                        switch (op.operationType()) {
                          case CREATE:
                            check.canCreateEntity(branch, op.identifiedKey(), actions);
                            break;
                          case UPDATE:
                            check.canUpdateEntity(branch, op.identifiedKey(), actions);
                            break;
                          case DELETE:
                            check.canDeleteEntity(branch, op.identifiedKey(), actions);
                            break;
                          default:
                            throw new UnsupportedOperationException(
                                "Unknown operation type " + op.operationType());
                        }
                      });
              check.checkAndThrow();
            },
            (k, c) -> {})
        .getCommitHash();
  }
}
