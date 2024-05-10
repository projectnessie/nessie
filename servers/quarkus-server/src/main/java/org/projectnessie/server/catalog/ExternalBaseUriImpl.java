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
package org.projectnessie.server.catalog;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import org.projectnessie.catalog.service.rest.ExternalBaseUri;

@RequestScoped
public class ExternalBaseUriImpl implements ExternalBaseUri {

  @Inject UriInfo uriInfo;

  @Override
  public URI externalBaseURI() {
    return uriInfo.getBaseUri();
  }
}
