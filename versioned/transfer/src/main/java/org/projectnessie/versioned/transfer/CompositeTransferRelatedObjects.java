/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.transfer;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.collections.ObjectHashSet;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.storage.common.logic.IdentifyHeadsAndForkPoints;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.transfer.related.TransferRelatedObjects;

final class CompositeTransferRelatedObjects implements TransferRelatedObjects {

  private final List<TransferRelatedObjects> transferRelatedObjectsImpls;

  /**
   * One "related" object might be referenced by multiple content objects or commits or references.
   * This implementation avoid exporting the same "related" object more than once. This set of
   * {@link ObjId}s is unbounded, like the collections in {@link IdentifyHeadsAndForkPoints}.
   */
  private final ObjectHashSet<ObjId> seen = new ObjectHashSet<>();

  public CompositeTransferRelatedObjects(List<URL> jarUrls) {
    // Exporter runs as a Quarkus CLI application, so "just adding additional jars via `java
    // -classpath`" doesn't work here. The artifacts that implement TransferRelatedObjects are
    // rather lightweight and only depend on code that's present in Nessie's server-admin-tool (in
    // the thread's context class loader).
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (!jarUrls.isEmpty()) {
      cl = new URLClassLoader(jarUrls.toArray(new URL[0]), cl);
    }
    this.transferRelatedObjectsImpls =
        ServiceLoader.load(TransferRelatedObjects.class, cl).stream()
            .map(ServiceLoader.Provider::get)
            .collect(Collectors.toList());
  }

  private Set<ObjId> filterSeen(Stream<ObjId> src) {
    return src.filter(seen::add).collect(Collectors.toSet());
  }

  @Override
  public Set<ObjId> repositoryRelatedObjects() {
    return filterSeen(
        transferRelatedObjectsImpls.stream().flatMap(i -> i.repositoryRelatedObjects().stream()));
  }

  @Override
  public Set<ObjId> commitRelatedObjects(CommitObj commitObj) {
    return filterSeen(
        transferRelatedObjectsImpls.stream()
            .flatMap(i -> i.commitRelatedObjects(commitObj).stream()));
  }

  @Override
  public Set<ObjId> contentRelatedObjects(Content content) {
    return filterSeen(
        transferRelatedObjectsImpls.stream()
            .flatMap(i -> i.contentRelatedObjects(content).stream()));
  }

  @Override
  public Set<ObjId> referenceRelatedObjects(Reference reference) {
    return filterSeen(
        transferRelatedObjectsImpls.stream()
            .flatMap(i -> i.referenceRelatedObjects(reference).stream()));
  }
}
