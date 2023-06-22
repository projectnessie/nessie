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
package org.projectnessie.restcatalog.service.resources;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;
import org.projectnessie.restcatalog.api.HttpProxy;
import org.projectnessie.restcatalog.service.TenantSpecific;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class NessieProxyResource implements HttpProxy {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieProxyResource.class);

  public static final List<String> REQUEST_HEADERS =
      Arrays.asList(
          "Content-Type",
          "Content-Encoding",
          "Accept",
          "Accept-Encoding",
          "Accept-Language",
          "Nessie-Client-Spec");
  public static final List<String> RESPONSE_HEADERS =
      Arrays.asList(
          "Content-Type",
          "Content-Length",
          "Content-Encoding",
          "Accept",
          "Expires",
          "Cache-Control",
          "Last-Modified",
          "ETag",
          "Vary");

  @Inject protected TenantSpecific tenantSpecific;

  @Inject protected Request request;
  @Inject protected HttpHeaders httpHeaders;
  @Inject protected UriInfo uriInfo;
  @Inject protected HttpClientHolder httpClientHolder;

  private URI proxyTo() {
    // Need the request URI, which has everything we need - the path and query parameters
    URI reqUri = uriInfo.getRequestUri();
    // "Our" base-URI is the one for this resource
    URI baseUri = uriInfo.getBaseUri().resolve("api/");

    // Relativize the request URI
    URI relativeUri = baseUri.relativize(reqUri);

    URI nessieApiBaseUri = tenantSpecific.nessieApiBaseUri();

    // Ensure that the resolved URI is not used to abuse this proxy for anything else than Nessie
    // Core REST API calls. Otherwise, it could be used to issue generic requests to other resources
    // on the same network.
    if (relativeUri.isAbsolute()) {
      // Status message isn't always propagated into the result
      throw new ClientErrorException(Response.Status.BAD_REQUEST);
    }

    // Resolve the relative request URI against the Nessie Core server base URI. This gives us the
    // properly encoded path and query parameters.
    return nessieApiBaseUri.resolve(relativeUri);
  }

  private void requestHeaders(BiConsumer<String, String> headerConsumer) {
    for (String hdr : REQUEST_HEADERS) {
      String value = httpHeaders.getHeaderString(hdr);
      if (value != null) {
        headerConsumer.accept(hdr, value);
      }
    }
  }

  private void responseHeaders(
      Function<String, List<String>> headerProvider, BiConsumer<String, String> headerConsumer) {
    for (String hdr : RESPONSE_HEADERS) {
      List<String> value = headerProvider.apply(hdr);
      if (value != null) {
        value.forEach(v -> headerConsumer.accept(hdr, v));
      }
    }
  }

  public Response doProxy(InputStream data) {
    try {
      URI target = proxyTo();

      HttpClient httpClient = httpClientHolder.httpClient();

      BodyPublisher bodyPublisher =
          data != null ? BodyPublishers.ofInputStream(() -> data) : BodyPublishers.noBody();

      HttpRequest.Builder httpRequest =
          HttpRequest.newBuilder(target)
              .timeout(httpClientHolder.requestTimeout())
              .method(request.getMethod(), bodyPublisher);
      requestHeaders(httpRequest::header);

      BodyHandler<InputStream> bodyHandler = BodyHandlers.ofInputStream();

      HttpResponse<InputStream> httpResponse = httpClient.send(httpRequest.build(), bodyHandler);

      ResponseBuilder responseBuilder = Response.status(httpResponse.statusCode());
      responseHeaders(httpResponse.headers()::allValues, responseBuilder::header);

      return responseBuilder.entity(httpResponse.body()).build();
    } catch (WebApplicationException e) {
      return e.getResponse();
    } catch (Exception e) {
      LOGGER.error(
          "Internal error during proxy of {} {}",
          request.getMethod(),
          uriInfo.getAbsolutePath(),
          e);
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
