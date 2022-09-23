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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.RequestContext;

public abstract class BaseHttpRequest extends org.projectnessie.client.http.HttpRequest {

  protected BaseHttpRequest(HttpRuntimeConfig config) {
    super(config);
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

  protected OutputStream wrapOutputStream(OutputStream base) throws IOException {
    return config.isDisableCompression()
        ? new BufferedOutputStream(base)
        : new GZIPOutputStream(base);
  }

  protected void writeBody(HttpRuntimeConfig config, OutputStream out, Object body)
      throws IOException {
    Class<?> bodyType = body.getClass();
    if (bodyType != String.class) {
      config.getMapper().writerFor(bodyType).writeValue(out, body);
    } else {
      // This is mostly used for testing bad/broken JSON
      out.write(((String) body).getBytes(StandardCharsets.UTF_8));
    }
  }
}
