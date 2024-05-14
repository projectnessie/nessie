/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.client.http.impl.jdk8;

import static org.projectnessie.client.http.impl.HttpUtils.applyHeaders;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.List;
import java.util.function.BiConsumer;
import javax.net.ssl.HttpsURLConnection;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpClientReadTimeoutException;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.impl.BaseHttpRequest;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.projectnessie.client.http.impl.RequestContextImpl;

/** Class to hold an ongoing HTTP request and its parameters/filters. */
final class UrlConnectionRequest extends BaseHttpRequest {

  UrlConnectionRequest(HttpRuntimeConfig config, URI baseUri) {
    super(config, baseUri);
  }

  @Override
  public HttpResponse executeRequest(Method method, Object body) throws HttpClientException {
    URI uri = uriBuilder.build();
    try {
      HttpURLConnection con = (HttpURLConnection) uri.toURL().openConnection();
      con.setReadTimeout(config.getReadTimeoutMillis());
      con.setConnectTimeout(config.getConnectionTimeoutMillis());
      if (con instanceof HttpsURLConnection) {
        ((HttpsURLConnection) con).setSSLSocketFactory(config.getSslContext().getSocketFactory());
      }
      RequestContext context = new RequestContextImpl(headers, uri, method, body);
      ResponseContext responseContext = new UrlConnectionResponseContext(con, uri);
      try {

        boolean doesOutput = prepareRequest(context);

        applyHeaders(headers, con);
        con.setRequestMethod(method.name());

        if (doesOutput) {
          con.setDoOutput(true);

          writeToOutputStream(context, con.getOutputStream());
        }
        con.connect();
        con.getResponseCode(); // call to ensure http request is complete

        List<BiConsumer<ResponseContext, Exception>> callbacks = context.getResponseCallbacks();
        if (callbacks != null) {
          callbacks.forEach(callback -> callback.accept(responseContext, null));
        }
      } catch (IOException e) {
        List<BiConsumer<ResponseContext, Exception>> callbacks = context.getResponseCallbacks();
        if (callbacks != null) {
          callbacks.forEach(callback -> callback.accept(null, e));
        }
        throw e;
      }

      config.getResponseFilters().forEach(responseFilter -> responseFilter.filter(responseContext));

      return config.responseFactory().make(responseContext, config.getMapper());
    } catch (ProtocolException e) {
      throw new HttpClientException(
          String.format("Cannot perform request against '%s'. Invalid protocol %s", uri, method),
          e);
    } catch (MalformedURLException e) {
      throw new HttpClientException(
          String.format("Cannot perform %s request. Malformed Url for %s", method, uri), e);
    } catch (SocketTimeoutException e) {
      throw new HttpClientReadTimeoutException(
          String.format(
              "Cannot finish %s request against '%s'. Timeout while waiting for response with a timeout of %ds",
              method, uri, config.getReadTimeoutMillis() / 1000),
          e);
    } catch (IOException e) {
      throw new HttpClientException(
          String.format("Failed to execute %s request against '%s'.", method, uri), e);
    } finally {
      cleanUp();
    }
  }

  @Override
  public HttpResponse get() throws HttpClientException {
    return executeRequest(Method.GET, null);
  }

  @Override
  public HttpResponse delete() throws HttpClientException {
    return executeRequest(Method.DELETE, null);
  }

  @Override
  public HttpResponse post(Object obj) throws HttpClientException {
    return executeRequest(Method.POST, obj);
  }

  @Override
  public HttpResponse put(Object obj) throws HttpClientException {
    return executeRequest(Method.PUT, obj);
  }

  @Override
  public HttpRequest resolveTemplate(String name, String value) {
    uriBuilder.resolveTemplate(name, value);
    return this;
  }
}
