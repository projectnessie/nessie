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

import static org.projectnessie.client.http.HttpUtils.ACCEPT_ENCODING;
import static org.projectnessie.client.http.HttpUtils.GZIP;
import static org.projectnessie.client.http.HttpUtils.HEADER_ACCEPT;
import static org.projectnessie.client.http.HttpUtils.HEADER_ACCEPT_ENCODING;
import static org.projectnessie.client.http.HttpUtils.HEADER_CONTENT_ENCODING;
import static org.projectnessie.client.http.HttpUtils.HEADER_CONTENT_TYPE;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.HttpsURLConnection;
import org.projectnessie.client.http.HttpClient.Method;

/** Class to hold an ongoing HTTP request and its parameters/filters. */
public class HttpRequest {

  private final HttpRuntimeConfig config;
  private final UriBuilder uriBuilder;
  private final HttpHeaders headers = new HttpHeaders();
  private String contentsType = "application/json; charset=utf-8";
  private String accept = "application/json; charset=utf-8";

  HttpRequest(HttpRuntimeConfig config) {
    this.uriBuilder = new UriBuilder(config.getBaseUri());
    this.config = config;
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

  public HttpRequest queryParam(String name, Integer value) {
    if (value != null) {
      this.uriBuilder.queryParam(name, value.toString());
    }
    return this;
  }

  public HttpRequest header(String name, String value) {
    headers.put(name, value);
    return this;
  }

  private HttpResponse executeRequest(Method method, Object body) throws HttpClientException {
    URI uri = uriBuilder.build();
    try {
      HttpURLConnection con = (HttpURLConnection) uri.toURL().openConnection();
      con.setReadTimeout(config.getReadTimeoutMillis());
      con.setConnectTimeout(config.getConnectionTimeoutMillis());
      if (con instanceof HttpsURLConnection) {
        ((HttpsURLConnection) con).setSSLSocketFactory(config.getSslContext().getSocketFactory());
      }
      RequestContext context = new RequestContext(headers, uri, method, body);
      ResponseContext responseContext = new ResponseContextImpl(con);
      try {
        headers.put(HEADER_ACCEPT, accept);

        boolean postOrPut = method == Method.PUT || method == Method.POST;

        if (postOrPut) {
          // Need to set the Content-Type even if body==null, otherwise the server responds with
          // RESTEASY003065: Cannot consume content type
          headers.put(HEADER_CONTENT_TYPE, contentsType);
        }

        boolean doesOutput = postOrPut && body != null;

        if (!config.isDisableCompression()) {
          headers.put(HEADER_ACCEPT_ENCODING, ACCEPT_ENCODING);
          if (doesOutput) {
            headers.put(HEADER_CONTENT_ENCODING, GZIP);
          }
        }

        config.getRequestFilters().forEach(a -> a.filter(context));
        headers.applyTo(con);
        con.setRequestMethod(method.name());

        if (doesOutput) {
          con.setDoOutput(true);

          OutputStream out = con.getOutputStream();
          try {
            if (!config.isDisableCompression()) {
              out = new GZIPOutputStream(out);
            }

            Class<?> bodyType = body.getClass();
            if (bodyType != String.class) {
              config.getMapper().writerFor(bodyType).writeValue(out, body);
            } else {
              // This is mostly used for testing bad/broken JSON
              out.write(((String) body).getBytes(StandardCharsets.UTF_8));
            }
          } finally {
            out.close();
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

      config.getResponseFilters().forEach(responseFilter -> responseFilter.filter(responseContext));

      return new HttpResponse(responseContext, config.getMapper());
    } catch (ProtocolException e) {
      throw new HttpClientException(
          String.format("Cannot perform request against '%s'. Invalid protocol %s", uri, method),
          e);
    } catch (JsonGenerationException | JsonMappingException e) {
      throw new HttpClientException(
          String.format(
              "Cannot serialize body of %s request against '%s'. Unable to serialize %s",
              method, uri, body.getClass()),
          e);
    } catch (MalformedURLException e) {
      throw new HttpClientException(
          String.format(
              "Cannot perform %s request. Malformed Url for %s", method, uriBuilder.build()),
          e);
    } catch (SocketTimeoutException e) {
      throw new HttpClientReadTimeoutException(
          String.format(
              "Cannot finish %s request against '%s'. Timeout while waiting for response with a timeout of %ds",
              method, uri, config.getReadTimeoutMillis() / 1000),
          e);
    } catch (IOException e) {
      throw new HttpClientException(
          String.format("Failed to execute %s request against '%s'.", method, uri), e);
    }
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
