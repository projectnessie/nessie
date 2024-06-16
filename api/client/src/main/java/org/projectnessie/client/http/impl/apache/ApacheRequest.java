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
package org.projectnessie.client.http.impl.apache;

import static org.projectnessie.client.http.impl.HttpUtils.HEADER_ACCEPT;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpClientResponseException;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.http.impl.BaseHttpRequest;
import org.projectnessie.client.http.impl.HttpHeaders.HttpHeader;
import org.projectnessie.client.http.impl.RequestContextImpl;

final class ApacheRequest extends BaseHttpRequest {

  private final ApacheHttpClient client;

  ApacheRequest(ApacheHttpClient apacheHttpClient, URI baseUri) {
    super(apacheHttpClient.config, baseUri);
    this.client = apacheHttpClient;
  }

  @Override
  public HttpResponse executeRequest(Method method, Object body) throws HttpClientException {

    URI uri = uriBuilder.build();

    HttpUriRequestBase request = new HttpUriRequestBase(method.name(), uri);

    RequestContext context = new RequestContextImpl(headers, uri, method, body);

    boolean doesOutput = prepareRequest(context);

    config.getRequestFilters().forEach(a -> a.filter(context));

    for (HttpHeader header : headers.allHeaders()) {
      for (String value : header.getValues()) {
        request.addHeader(header.getName(), value);
      }
    }

    request.addHeader(HEADER_ACCEPT, accept);
    if (doesOutput) {
      HttpEntity entity =
          HttpEntities.create(
              os -> writeToOutputStream(context, os), ContentType.parse(contentsType));
      request.setEntity(entity);
    }

    ClassicHttpResponse response = null;
    try {
      response = client.client.executeOpen(null, request, null);

      ApacheResponseContext responseContext = new ApacheResponseContext(response, uri);

      List<BiConsumer<ResponseContext, Exception>> callbacks = context.getResponseCallbacks();
      if (callbacks != null) {
        callbacks.forEach(callback -> callback.accept(responseContext, null));
      }

      config.getResponseFilters().forEach(responseFilter -> responseFilter.filter(responseContext));

      if (response.getCode() >= 400) {
        Status status = Status.fromCode(response.getCode());
        throw new HttpClientResponseException(
            String.format("%s request to %s failed with HTTP/%d", method, uri, response.getCode()),
            status);
      }

      response = null;
      return new HttpResponse(responseContext, config.getMapper());
    } catch (ConnectTimeoutException e) {
      throw new HttpClientException(
          String.format(
              "Timeout connecting to '%s' after %ds",
              uri, config.getConnectionTimeoutMillis() / 1000),
          e);
    } catch (IOException e) {
      throw new HttpClientException(e);
    } finally {
      if (response != null) {
        try {
          response.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }
}
