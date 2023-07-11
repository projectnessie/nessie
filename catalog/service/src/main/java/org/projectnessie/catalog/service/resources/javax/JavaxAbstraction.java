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
package org.projectnessie.catalog.service.resources.javax;

import java.net.URI;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.UriInfo;
import org.projectnessie.catalog.service.resources.RestAbstraction;

@RequestScoped
public class JavaxAbstraction implements RestAbstraction {

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
