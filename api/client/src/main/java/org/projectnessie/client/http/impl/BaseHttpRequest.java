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
package org.projectnessie.client.http.impl;

import static org.projectnessie.client.http.impl.HttpUtils.GZIP;
import static org.projectnessie.client.http.impl.HttpUtils.GZIP_DEFLATE;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_ACCEPT;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_ACCEPT_ENCODING;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_CONTENT_ENCODING;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_CONTENT_TYPE;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.zip.GZIPOutputStream;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpClientReadTimeoutException;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;

public abstract class BaseHttpRequest extends HttpRequest {

  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  protected BaseHttpRequest(HttpRuntimeConfig config, URI baseUri) {
    super(config, baseUri);
  }

  @Override
  public HttpResponse executeRequest(Method method, Object body) throws HttpClientException {
    URI uri = uriBuilder.build();
    RequestContext requestContext = new RequestContextImpl(headers, uri, method, body);
    ResponseContext responseContext = null;
    RuntimeException error = null;
    try {
      prepareRequest(requestContext);
      responseContext = processResponse(uri, method, body, requestContext);
      processResponseFilters(responseContext);
      return config.responseFactory().make(responseContext, config.getMapper());
    } catch (RuntimeException e) {
      error = e;
    } finally {
      error = processCallbacks(requestContext, responseContext, error);
      cleanUp(responseContext, error);
    }
    throw error;
  }

  protected void prepareRequest(RequestContext context) {
    headers.put(HEADER_ACCEPT, accept);

    Method method = context.getMethod();
    if (method == Method.PUT || method == Method.POST) {
      // Need to set the Content-Type even if body==null, otherwise the server responds with
      // RESTEASY003065: Cannot consume content type
      headers.put(HEADER_CONTENT_TYPE, contentsType);
    }

    if (!config.isDisableCompression()) {
      headers.put(HEADER_ACCEPT_ENCODING, GZIP_DEFLATE);
      if (context.doesOutput()) {
        headers.put(HEADER_CONTENT_ENCODING, GZIP);
      }
    }

    processRequestFilters(context);

    HttpAuthentication auth = this.auth;
    if (auth != null) {
      auth.applyToHttpRequest(context);
      auth.start();
    }
  }

  protected ResponseContext processResponse(
      URI uri, Method method, Object body, RequestContext requestContext) {
    try {
      return sendAndReceive(uri, method, body, requestContext);
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
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract ResponseContext sendAndReceive(
      URI uri, Method method, Object body, RequestContext requestContext)
      throws IOException, InterruptedException;

  protected void processRequestFilters(RequestContext requestContext) {
    if (!bypassFilters) {
      config.getRequestFilters().forEach(requestFilter -> requestFilter.filter(requestContext));
    }
  }

  protected void processResponseFilters(ResponseContext responseContext) {
    if (!bypassFilters) {
      config.getResponseFilters().forEach(responseFilter -> responseFilter.filter(responseContext));
    }
  }

  protected RuntimeException processCallbacks(
      RequestContext requestContext,
      @Nullable ResponseContext responseContext,
      @Nullable RuntimeException originalError) {
    List<BiConsumer<ResponseContext, Exception>> callbacks = requestContext.getResponseCallbacks();
    RuntimeException error = originalError;
    if (callbacks != null) {
      for (BiConsumer<ResponseContext, Exception> callback : callbacks) {
        try {
          callback.accept(responseContext, originalError);
        } catch (RuntimeException e) {
          if (error == null) {
            error = e;
          } else {
            error.addSuppressed(e);
          }
        }
      }
    }
    return error;
  }

  protected void cleanUp(ResponseContext responseContext, RuntimeException error) {
    try {
      HttpAuthentication auth = this.auth;
      if (auth != null) {
        auth.close();
      }
    } finally {
      if (responseContext != null) {
        responseContext.close(error);
      }
    }
  }

  protected void writeToOutputStream(RequestContext context, OutputStream outputStream)
      throws IOException {
    Object body = context.getBody().orElseThrow(() -> new IllegalStateException("No request body"));
    try (OutputStream out = wrapOutputStream(outputStream)) {
      if (context.isFormEncoded()) {
        writeFormData(out, body);
      } else {
        writeBody(out, body);
      }
    } catch (JsonGenerationException | JsonMappingException e) {
      throw new HttpClientException(
          String.format(
              "Cannot serialize body of %s request against '%s'. Unable to serialize %s",
              context.getMethod(), context.getUri(), body.getClass()),
          e);
    }
  }

  private OutputStream wrapOutputStream(OutputStream base) throws IOException {
    return config.isDisableCompression() ? base : new GZIPOutputStream(base);
  }

  private void writeBody(OutputStream out, Object body) throws IOException {
    Class<?> bodyType = body.getClass();
    if (bodyType != String.class) {
      ObjectWriter writer = config.getMapper().writer();
      if (config.getJsonView() != null) {
        writer = writer.withView(config.getJsonView());
      }
      writer.forType(bodyType).writeValue(out, body);
    } else {
      // This is mostly used for testing bad/broken JSON
      out.write(((String) body).getBytes(StandardCharsets.UTF_8));
    }
  }

  private void writeFormData(OutputStream out, Object body) throws IOException {
    ObjectMapper mapper = config.getMapper();
    Map<String, Object> map = mapper.convertValue(body, MAP_TYPE);
    boolean first = true;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      if (!first) {
        out.write('&');
      } else {
        first = false;
      }
      String key = URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8.name());
      String value =
          URLEncoder.encode(
              mapper.convertValue(entry.getValue(), String.class), StandardCharsets.UTF_8.name());
      out.write(key.getBytes(StandardCharsets.UTF_8));
      out.write('=');
      out.write(value.getBytes(StandardCharsets.UTF_8));
    }
  }
}
