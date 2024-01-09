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

import static org.projectnessie.client.http.impl.HttpUtils.ACCEPT_ENCODING;
import static org.projectnessie.client.http.impl.HttpUtils.GZIP;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_ACCEPT;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_ACCEPT_ENCODING;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_CONTENT_ENCODING;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_CONTENT_TYPE;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.RequestContext;

public abstract class BaseHttpRequest extends HttpRequest {

  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  protected BaseHttpRequest(HttpRuntimeConfig config, URI baseUri) {
    super(config, baseUri);
  }

  protected boolean prepareRequest(RequestContext context) {
    headers.put(HEADER_ACCEPT, accept);

    Method method = context.getMethod();
    boolean postOrPut = method == Method.PUT || method == Method.POST;
    if (postOrPut) {
      // Need to set the Content-Type even if body==null, otherwise the server responds with
      // RESTEASY003065: Cannot consume content type
      headers.put(HEADER_CONTENT_TYPE, contentsType);
    }

    boolean doesOutput = postOrPut && context.getBody().isPresent();
    if (!config.isDisableCompression()) {
      headers.put(HEADER_ACCEPT_ENCODING, ACCEPT_ENCODING);
      if (doesOutput) {
        headers.put(HEADER_CONTENT_ENCODING, GZIP);
      }
    }
    config.getRequestFilters().forEach(a -> a.filter(context));

    return doesOutput;
  }

  protected void writeToOutputStream(RequestContext context, OutputStream outputStream)
      throws IOException {
    Object body = context.getBody().orElseThrow(NullPointerException::new);
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
