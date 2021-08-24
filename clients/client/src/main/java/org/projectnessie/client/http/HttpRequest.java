/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.client.http;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import org.projectnessie.client.http.HttpClient.Method;

/** Class to hold an ongoing HTTP request and its parameters/filters. */
public class HttpRequest {

  private final UriBuilder uriBuilder;
  private final ObjectMapper mapper;
  private final int readTimeoutMillis;
  private final int connectionTimeoutMillis;
  private final Map<String, Set<String>> headers = new HashMap<>();
  private final List<RequestFilter> requestFilters;
  private final List<ResponseFilter> responseFilters;
  private SSLContext sslContext;
  private String contentsType = "application/json; charset=utf-8";
  private String accept = "application/json; charset=utf-8";

  HttpRequest(
      URI baseUri,
      ObjectMapper mapper,
      List<RequestFilter> requestFilters,
      List<ResponseFilter> responseFilters,
      SSLContext context,
      int readTimeoutMillis,
      int connectionTimeoutMillis) {
    this.uriBuilder = new UriBuilder(baseUri);
    this.mapper = mapper;
    this.readTimeoutMillis = readTimeoutMillis;
    this.connectionTimeoutMillis = connectionTimeoutMillis;
    this.requestFilters = requestFilters;
    this.responseFilters = responseFilters;
    this.sslContext = context;
  }

  static void putHeader(String key, String value, Map<String, Set<String>> headers) {
    if (!headers.containsKey(key)) {
      headers.put(key, new HashSet<>());
    }
    headers.get(key).add(value);
  }

  public HttpRequest contentsType(String contentsType) {
    this.contentsType = contentsType;
    return this;
  }

  public HttpRequest accept(String contentsType) {
    this.accept = contentsType;
    return this;
  }

  public HttpRequest path(String path) {
    this.uriBuilder.path(path);
    return this;
  }

  public HttpRequest queryParam(String name, String value) {
    this.uriBuilder.queryParam(name, value);
    return this;
  }

  public HttpRequest header(String name, String value) {
    putHeader(name, value, headers);
    return this;
  }

  private HttpResponse executeRequest(Method method, Object body) throws HttpClientException {
    try {
      URI uri = uriBuilder.build();
      HttpURLConnection con = (HttpURLConnection) uri.toURL().openConnection();
      con.setReadTimeout(readTimeoutMillis);
      con.setConnectTimeout(connectionTimeoutMillis);
      if (con instanceof HttpsURLConnection) {
        ((HttpsURLConnection) con).setSSLSocketFactory(sslContext.getSocketFactory());
      }
      RequestContext context = new RequestContext(headers, uri, method, body);
      ResponseContext responseContext = new ResponseContextImpl(con);
      try {
        requestFilters.forEach(a -> a.filter(context));
        putHeader("Accept", accept, headers);
        headers.entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(x -> new SimpleImmutableEntry<>(e.getKey(), x)))
            .forEach(x -> con.setRequestProperty(x.getKey(), x.getValue()));
        con.setRequestMethod(method.name());
        if (method.equals(Method.PUT) || method.equals(Method.POST)) {
          // Need to set the Content-Type even if body==null, otherwise the server responds with
          // RESTEASY003065: Cannot consume content type
          con.setRequestProperty("Content-Type", contentsType);
          if (body != null) {
            con.setDoOutput(true);
            Class<?> bodyType = body.getClass();
            if (bodyType != String.class) {
              mapper.writerFor(bodyType).writeValue(con.getOutputStream(), body);
            } else {
              // This is mostly used for testing bad/broken JSON
              con.getOutputStream().write(((String) body).getBytes(StandardCharsets.UTF_8));
            }
          }
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

      responseFilters.forEach(responseFilter -> responseFilter.filter(responseContext));

      return new HttpResponse(responseContext, mapper);
    } catch (ProtocolException e) {
      throw new HttpClientException(
          String.format("Cannot perform request. Invalid protocol %s", method), e);
    } catch (JsonGenerationException | JsonMappingException e) {
      throw new HttpClientException(
          String.format(
              "Cannot serialize body of request. Unable to serialize %s", body.getClass()),
          e);
    } catch (MalformedURLException e) {
      throw new HttpClientException(
          String.format("Cannot perform request. Malformed Url for %s", uriBuilder.build()), e);
    } catch (SocketTimeoutException e) {
      throw new HttpClientReadTimeoutException(
          String.format(
              "Cannot finish request. Timeout while waiting for response with a timeout of %ds",
              readTimeoutMillis / 1000),
          e);
    } catch (IOException e) {
      throw new HttpClientException(e);
    }
  }

  public HttpRequest setSslContext(SSLContext context) {
    this.sslContext = context;
    return this;
  }

  public HttpResponse get() throws HttpClientException {
    return executeRequest(Method.GET, null);
  }

  public HttpResponse delete() throws HttpClientException {
    return executeRequest(Method.DELETE, null);
  }

  public HttpResponse post(Object obj) throws HttpClientException {
    return executeRequest(Method.POST, obj);
  }

  public HttpResponse put(Object obj) throws HttpClientException {
    return executeRequest(Method.PUT, obj);
  }

  public HttpRequest resolveTemplate(String name, String value) {
    uriBuilder.resolveTemplate(name, value);
    return this;
  }
}
