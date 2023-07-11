/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.service.resources.jakarta;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Request;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import org.projectnessie.catalog.service.resources.RestAbstraction;

@RequestScoped
public class JakartaAbstraction implements RestAbstraction {

  @Inject protected UriInfo uriInfo;
  @Inject protected Request request;
  @Inject protected HttpHeaders httpHeaders;

  @Override
  public URI baseUri() {
    return uriInfo.getBaseUri();
  }

  @Override
  public URI requestUri() {
    return uriInfo.getRequestUri();
  }

  @Override
  public String requestMethod() {
    return request.getMethod();
  }

  @Override
  public String headerString(String header) {
    return httpHeaders.getHeaderString(header);
  }
}
