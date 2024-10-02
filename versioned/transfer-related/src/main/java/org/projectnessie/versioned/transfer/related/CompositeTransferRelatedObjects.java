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
package org.projectnessie.versioned.transfer.related;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

public final class CompositeTransferRelatedObjects implements TransferRelatedObjects {

  private final List<TransferRelatedObjects> transferRelatedObjectsImpls;
  private final Predicate<ObjId> filter;

  public static TransferRelatedObjects createCompositeTransferRelatedObjects() {
    return createCompositeTransferRelatedObjects(List.of(), x -> true);
  }

  public static TransferRelatedObjects createCompositeTransferRelatedObjects(List<URL> jarUrls) {
    return createCompositeTransferRelatedObjects(jarUrls, x -> true);
  }

  public static TransferRelatedObjects createCompositeTransferRelatedObjects(
      List<URL> jarUrls, Predicate<ObjId> filter) {
    return new CompositeTransferRelatedObjects(jarUrls, filter);
  }

  private CompositeTransferRelatedObjects(List<URL> jarUrls, Predicate<ObjId> filter) {
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
    this.filter = filter;
  }

  private Set<ObjId> filter(Stream<ObjId> src) {
    return src.filter(filter).collect(Collectors.toSet());
  }

  @Override
  public Set<ObjId> repositoryRelatedObjects() {
    return filter(
        transferRelatedObjectsImpls.stream().flatMap(i -> i.repositoryRelatedObjects().stream()));
  }

  @Override
  public Set<ObjId> commitRelatedObjects(CommitObj commitObj) {
    return filter(
        transferRelatedObjectsImpls.stream()
            .flatMap(i -> i.commitRelatedObjects(commitObj).stream()));
  }

  @Override
  public Set<ObjId> contentRelatedObjects(Content content) {
    return filter(
        transferRelatedObjectsImpls.stream()
            .flatMap(i -> i.contentRelatedObjects(content).stream()));
  }

  @Override
  public Set<ObjId> referenceRelatedObjects(Reference reference) {
    return filter(
        transferRelatedObjectsImpls.stream()
            .flatMap(i -> i.referenceRelatedObjects(reference).stream()));
  }
}
