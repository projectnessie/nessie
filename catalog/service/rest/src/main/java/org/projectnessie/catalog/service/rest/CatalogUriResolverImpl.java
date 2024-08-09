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
package org.projectnessie.catalog.service.rest;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.service.api.CatalogService;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

class CatalogUriResolverImpl implements CatalogService.CatalogUriResolver {
  private final URI baseUri;

  CatalogUriResolverImpl(ExternalBaseUri requestUri) {
    this.baseUri = requestUri.catalogBaseURI();
  }

  @Override
  public URI icebergSnapshot(
      Reference effectiveReference, ContentKey key, NessieEntitySnapshot<?> snapshot) {
    return baseUri.resolve(
        "trees/"
            + encode(effectiveReference.toPathString(), UTF_8)
            + "/snapshot/"
            + encode(key.toPathStringEscaped(), UTF_8)
            + "?format=iceberg");
  }
}
