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
package org.projectnessie.restcatalog.service.resources.javax;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpResponse;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;
import org.projectnessie.restcatalog.api.javax.HttpProxy;
import org.projectnessie.restcatalog.service.AbstractNessieProxyResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Java EE ({@code javax.*}) specific proxy resource. */
@RequestScoped
public class NessieProxyResource extends AbstractNessieProxyResource implements HttpProxy {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieProxyResource.class);

  @Inject protected Request request;
  @Inject protected HttpHeaders httpHeaders;
  @Inject protected UriInfo uriInfo;

  @Override
  protected URI baseUri() {
    return uriInfo.getBaseUri().resolve("api/");
  }

  @Override
  protected URI requestUri() {
    return uriInfo.getRequestUri();
  }

  @Override
  protected String requestMethod() {
    return request.getMethod();
  }

  @Override
  protected String headerString(String header) {
    return httpHeaders.getHeaderString(header);
  }

  public Response doProxy(InputStream data) {
    try {
      HttpResponse<InputStream> httpResponse = proxyRequest(data);

      ResponseBuilder responseBuilder = Response.status(httpResponse.statusCode());
      responseHeaders(httpResponse.headers()::allValues, responseBuilder::header);

      return responseBuilder.entity(httpResponse.body()).build();
    } catch (WebApplicationException e) {
      return e.getResponse();
    } catch (Exception e) {
      LOGGER.error("Internal error during proxy of {} {}", requestMethod(), requestUri(), e);
      return new ServerErrorException("Internal error", Response.Status.INTERNAL_SERVER_ERROR)
          .getResponse();
    }
  }

  @Override
  public Response doHead(String path) {
    return doProxy(null);
  }

  @Override
  public Response doGet(String path) {
    return doProxy(null);
  }

  @Override
  public Response doPost(String path, InputStream data) {
    return doProxy(data);
  }

  @Override
  public Response doPut(String path, InputStream data) {
    return doProxy(data);
  }

  @Override
  public Response doDelete(String path) {
    return doProxy(null);
  }
}
